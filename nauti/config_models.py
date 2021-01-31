#      Copyright (C) 2020  Jeremy Schulman
#
#      This program is free software: you can redistribute it and/or modify
#      it under the terms of the GNU General Public License as published by
#      the Free Software Foundation, either version 3 of the License, or
#      (at your option) any later version.
#
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU General Public License for more details.
#
#      You should have received a copy of the GNU General Public License
#      along with this program.  If not, see <https://www.gnu.org/licenses/>.

# -----------------------------------------------------------------------------
# System Imports
# -----------------------------------------------------------------------------

from typing import Optional, List, Dict, Any, Union
from pathlib import Path
from importlib.machinery import SourceFileLoader, FileFinder

# -----------------------------------------------------------------------------
# Public Imports
# -----------------------------------------------------------------------------

from pydantic import BaseModel, Extra, Field, root_validator

from pydantic_env.models import NoExtraBaseModel, EnvSecretStr, EnvUrl
from bidict import bidict, ValueDuplicationError

import toml

# -----------------------------------------------------------------------------
# Private Imports
# -----------------------------------------------------------------------------

from nauti.entrypoints import load_plugins

# -----------------------------------------------------------------------------
# Exports
# -----------------------------------------------------------------------------

__all__ = [
    "ConfigModel",
    "TokenCredential",
    "ClientCredential",
    "SourcesModel",
    "CollectionsModel",
    "CollectionSourceModel",
]


# -----------------------------------------------------------------------------
#
#                              CODE BEGINS
#
# -----------------------------------------------------------------------------


class BiDict(Dict):
    """
    This class is meant to allow the User to define either a bi-direction map or
    a unidirectional mapping.  The `bidict` is first attempted, and if any
    duplicates are found then fallback to using `dict`
    """

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        try:
            return bidict(v)
        except ValueDuplicationError:
            # TODO: perhaps add a warning log if some form of extra --debug flag
            #       is used to detect these conditions.
            return dict(v)
            # cnt = Counter(v.values())
            # dups = {key for key, val in cnt.items() if val > 1}
            # raise ValueError(f"map contains duplicates: {str(dups)}")


class TokenCredential(NoExtraBaseModel):
    token: EnvSecretStr


class ClientCredential(NoExtraBaseModel):
    client_id: EnvSecretStr
    client_secret: EnvSecretStr


AnyCredentialsModel = Union[TokenCredential, ClientCredential]


# -----------------------------------------------------------------------------
#
#                                Sources
#
# -----------------------------------------------------------------------------


class SourceInstanceModel(BaseModel):
    """ A specific source instance, as there could be more than one """

    url: EnvUrl
    credentials: AnyCredentialsModel
    options: Optional[Dict[str, Any]] = Field(default_factory=dict)


class SourcesModel(BaseModel):
    default: SourceInstanceModel
    vars: Optional[Dict[str, EnvSecretStr]]
    expands: Optional[Dict[str, BiDict]]
    maps: Optional[Dict[str, BiDict]]


# -----------------------------------------------------------------------------
#
#                                Collections
#
# -----------------------------------------------------------------------------


class CollectionSourceModel(BaseModel):
    maps: Dict[str, BiDict]


class CollectionsModel(BaseModel):

    # The BaseModel defines the attribute `fields` so we are not suppoed to use
    # that by default.  The "workaround" is to alias a different attribute name,
    # in this case using `fields_`, and then override the `fields` property. The
    # pydantic library depcreciated the use of `fields` in favor of `__fields__`
    # so I presume this workaround is OK.

    name: Optional[str]
    fields_: Optional[Dict[str, Any]] = Field(alias="fields")
    sources: Optional[Dict[str, CollectionSourceModel]]
    options: Optional[Dict[str, Any]] = Field(default_factory=dict)

    @property
    def fields(self):
        return self.fields_


class ConfigModel(BaseModel):
    class Config:
        extra = Extra.allow

    config_file: str
    domain_names: Optional[List[str]]
    sources: Dict[str, SourcesModel]
    collections: Dict[str, CollectionsModel]

    @root_validator(pre=True)
    def _root_validate(cls, values):
        """
        This root pre-validator is used to load the secondary toml configuration
        files so that the content can be parsed appropriately.
        """
        cfg_dir = Path(values["config_file"]).parent

        values["sources"] = {
            name: _load_config(cfg_dir, name) for name in values["sources"]
        }

        values["collections"] = {
            name: _load_collection_config(cfg_dir, name)
            for name in values["collections"]
        }

        _load_plugins(cfg_dir)
        return values


# -----------------------------------------------------------------------------
#
#                              PRIVATE FUNCTIONS
#
# -----------------------------------------------------------------------------


# def _load_item_config(cfg_dir, item_name, item_cls):
#     file_p = cfg_dir.joinpath(item_name + ".toml")
#     content = toml.load(file_p.open())
#     name = next(iter(content))
#     return item_cls.parse_obj(content[name])


def _load_collection_config(cfg_dir, item_name):
    file_p = cfg_dir.joinpath(item_name + ".toml")
    return toml.load(file_p.open()) if file_p.exists() else {}


def _load_config(cfg_dir, item_name):
    file_p = cfg_dir.joinpath(item_name + ".toml")
    if not file_p.exists():
        return {}

    content = toml.load(file_p.open())
    return next(iter(content.values()))


def _load_plugins(cfg_dir: Path):
    """
    This function will load all of the python modules found in the given cfg_dir
    so that any User defined plugins are brought into the system and registered.
    """
    load_plugins()

    plugins_dir = cfg_dir.joinpath("plugins")
    if not plugins_dir.is_dir():
        return

    finder = FileFinder(str(plugins_dir), (SourceFileLoader, [".py"]))  # noqa

    for py_file in plugins_dir.glob("*.py"):
        mod_name = py_file.stem
        finder.find_spec(mod_name).loader.load_module(mod_name)
