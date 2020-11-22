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

from typing import List
from pathlib import Path
from importlib.machinery import SourceFileLoader


NAUTI_EP_SOURCES = "nauti.sources"
NAUTI_EP_COLLECTIONS = "nauti.collections"
NAUTI_EP_TASKS = "nauti.tasks"


def find_collection_entrypoints(path: str) -> List[str]:
    from nauti.collection import CollectionMixin

    eps = list()

    for mod_fp in Path(path).glob("[!_]*.py"):
        mod_pxfp = str(mod_fp.as_posix())
        mod_name = mod_pxfp.replace("/", ".")
        mod = SourceFileLoader(mod_name, mod_pxfp).load_module()
        for exported in getattr(mod, "__all__", []):
            cls = getattr(mod, exported)
            if issubclass(cls, CollectionMixin) and cls != CollectionMixin:
                eps.append(f"{cls.name} = {mod.__package__}:{exported}")

    return eps


def find_source_entrypoints(path: str) -> List[str]:
    from nauti.source import Source

    eps = list()

    for mod_fp in Path(path).glob("[!_]*.py"):
        mod_pxfp = str(mod_fp.as_posix())
        mod_name = mod_pxfp.replace("/", ".")
        mod = SourceFileLoader(mod_name, mod_pxfp).load_module()

        for exported in getattr(mod, "__all__", []):
            cls = getattr(mod, exported)
            if issubclass(cls, Source) and cls != Source:
                eps.append(f"{cls.name} = {mod.__package__}:{exported}")

    return eps
