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

# -----------------------------------------------------------------------------
# Public Imports
# -----------------------------------------------------------------------------

from pydantic import BaseModel, Extra, Field, root_validator

from pydantic_env.models import NoExtraBaseModel, EnvSecretStr, EnvUrl
from bidict import bidict

import toml

# -----------------------------------------------------------------------------
# Exports
# -----------------------------------------------------------------------------

__all__ = ["ConfigModel", "TokenCredential", "ClientCredential", "SourcesModel"]

# -----------------------------------------------------------------------------
#
#                              CODE BEGINS
#
# -----------------------------------------------------------------------------


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


# -----------------------------------------------------------------------------
#
#                                Collections
#
# -----------------------------------------------------------------------------


class BiDict(Dict):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        return bidict(v)


class CollectionSourceModel(BaseModel):
    maps: Dict[str, BiDict]


class CollectionsModel(BaseModel):

    # The BaseModel defines the attribute `fields` so we are not suppoed to use
    # that by default.  The "workaround" is to alias a different attribute name,
    # in this case using `fields_`, and then override the `fields` property. The
    # pydantic library depcreciated the use of `fields` in favor of `__fields__`
    # so I presume this workaround is OK.

    name: str
    fields_: Optional[Dict[str, Any]] = Field(alias="fields")
    sources: Dict[str, CollectionSourceModel]

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
    return toml.load(file_p.open())


def _load_config(cfg_dir, item_name):
    file_p = cfg_dir.joinpath(item_name + ".toml")
    content = toml.load(file_p.open())
    return next(iter(content.values()))
