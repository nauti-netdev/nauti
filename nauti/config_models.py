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

from pydantic import (
    BaseModel, Extra,
    validator, Field
)
from pydantic_env.models import (
    NoExtraBaseModel, EnvSecretStr,
    EnvUrl
)

import toml

# -----------------------------------------------------------------------------
# Exports
# -----------------------------------------------------------------------------

__all__ = [
    'ConfigModel',
    'TokenCredential', 'ClientCredential',
    'SourcesModel'
]

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


class CollectionsModel(BaseModel):

    # The BaseModel defines the attribute `fields` so we are not suppoed to use
    # that by default.  The "workaround" is to alias a different attribute name,
    # in this case using `fields_`, and then override the `fields` property. The
    # pydantic library depcreciated the use of `fields` in favor of `__fields__`
    # so I presume this workaround is OK.

    fields_: Dict[str, Any] = Field(alias='fields')
    maps: Dict[str, Dict]

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

    @validator('sources', pre=True)
    def _each_source(cls, v, values):
        par_dir = Path(values['config_file']).parent
        return {name: _load_item_config(par_dir, name, SourcesModel) for name in v}

    @validator('collections', pre=True)
    def _each_collection(cls, v, values):
        par_dir = Path(values['config_file']).parent
        return {name: _load_item_config(par_dir, name, CollectionsModel) for name in v}


# -----------------------------------------------------------------------------
#
#                              PRIVATE FUNCTIONS
#
# -----------------------------------------------------------------------------

def _load_item_config(cfg_dir, item_name, item_cls):
    file_p = cfg_dir.joinpath(item_name + ".toml")
    content = toml.load(file_p.open())
    name = next(iter(content))
    return item_cls.parse_obj(content[name])
