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

from abc import ABC
from typing import Coroutine
from pkg_resources import iter_entry_points


from nauti.igather import igather
from nauti.entrypoints import NAUTI_EP_SOURCES
from nauti.config import get_config


__all__ = ["Source", "get_source"]


class Source(ABC):
    name = None
    client_class = None

    async def login(self, *vargs, **kwargs):
        raise NotImplementedError()

    async def logout(self):
        raise NotImplementedError()

    @property
    def is_connected(self):
        raise NotImplementedError()

    @staticmethod
    async def update(updates, callback, creator):

        tasks = dict()
        callback = callback or (lambda _k, _t: True)

        for key, value in updates.items():
            if (coro := creator(key, value)) is None:
                continue

            if not isinstance(coro, Coroutine):
                raise RuntimeError("Source.update requires a coroutine")

            tasks[coro] = (key, value)

        async for orig_coro, res in igather(tasks, limit=100):
            item = tasks[orig_coro]
            callback(item, res)


def get_source(name: str, source_name=None, **kwargs) -> Source:
    """
    Return the Source class designated by the 'name' field.  If a source by `name` is not
    found, then raise RuntimeError.

    Parameters
    ----------
    name: str
        The name of the source type, for example "netbox" or "ipfabric".

    source_name: str
        The specific instance name for the source, as would be found
        in the Config.sources structure.  If `instance_name` is not
        provided, then the `default` config is used.

    """
    if (ep := next(iter_entry_points(NAUTI_EP_SOURCES, name), None)) is None:
        raise RuntimeError(f"ERROR:NOT-FOUND: nauti source: {name}")

    cfg = get_config()
    src_cfg = cfg.sources[name]
    src_inst_cfg = src_cfg.copy()
    cls = ep.load()
    return cls(source_config=src_inst_cfg, **kwargs)
