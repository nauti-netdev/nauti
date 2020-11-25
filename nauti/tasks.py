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

from functools import wraps
from pkg_resources import iter_entry_points
from abc import ABC

from nauti.source import get_source
from nauti.collection import get_collection
from nauti.entrypoints import NAUTI_EP_TASKS

_registered_sync_tasks = dict()


def register_sync_task(source, target, collection):
    def decorate(coro):
        @wraps(coro)
        async def wrapper(*vargs, **kwargs):
            src_col = get_collection(get_source(source), collection)
            trg_col = get_collection(get_source(target), collection)

            async with src_col.source, trg_col.source:
                return await coro(src_col, trg_col, *vargs, **kwargs)

        _registered_sync_tasks[("sync", source, target, collection)] = wrapper
        return wrapper

    return decorate


def load_task_entrypoints():
    for ep in iter_entry_points(NAUTI_EP_TASKS):
        ep.load()


def get_task(*key_fields):
    return _registered_sync_tasks[key_fields]


# -----------------------------------------------------------------------------
# Register a Sync filter class used for the 'sync' tasks
# -----------------------------------------------------------------------------


_registered_sync_filters = dict()


class SyncCollectionFetchFilters(ABC):
    # The name of the origin source kind, for example "netbox"
    origin = None

    # The name of the target source kind, for example "clearpass"
    target = None

    # The name of the collection
    collection = None

    # The set of field names used to collect & sync
    fields = None

    # The set of field keys
    key_fields = None

    @staticmethod
    def origin_fetch_filter():
        """ returns a source specific fetch filter for the origin source """
        pass

    @staticmethod
    def origin_key_filter(item: dict):
        """ returns bool if the given item fields should be included in the key formation """
        return True

    @staticmethod
    def target_fetch_filter():
        """ returns a source specific fetch filter for the target source"""
        pass

    @staticmethod
    def target_key_filter(item: dict):
        """ return bool if the given item fields should be included in the key formation """
        return True


def register_sync_filter(name="default"):
    def decorator(cls):
        key = ("sync", name, cls.origin, cls.target, cls.collection)
        _registered_sync_filters[key] = cls
        return cls

    return decorator


def get_filter(kind, origin, target, collection, name="default"):
    key = ("sync", name, origin, target, collection)
    return _registered_sync_filters.get(key)
