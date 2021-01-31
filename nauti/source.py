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
from typing import Coroutine, Optional


from nauti.igather import igather
from nauti.config import get_config
from nauti.config_models import SourcesModel

__all__ = ["Source", "get_source"]


class Source(ABC):
    name = None
    client_class = None

    def __init__(self, config: Optional[SourcesModel] = None, **kwargs):
        self.client = None
        self.config = config

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

    async def __aenter__(self):
        await self.login()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.logout()


def get_source(name: str, **kwargs) -> Source:
    """
    Return the Source class designated by the 'name' field.  If a source by `name` is not
    found, then raise RuntimeError.

    Parameters
    ----------
    name: str
        The name of the source type, for example "netbox" or "ipfabric".
    """

    source_cls = next(
        (cls for cls in Source.__subclasses__() if cls.name == name), None
    )
    if not source_cls:
        raise RuntimeError(f"ERROR:NOT-FOUND: nauti source: {name}")

    cfg = get_config()
    src_cfg = cfg.sources[name]
    src_inst_cfg = src_cfg.copy()
    return source_cls(config=src_inst_cfg, **kwargs)
