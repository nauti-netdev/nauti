# -----------------------------------------------------------------------------
# System Imports
# -----------------------------------------------------------------------------

from abc import ABC

# -----------------------------------------------------------------------------
# System Imports
# -----------------------------------------------------------------------------

from nauti.collection import Collection
from .registrar import _registered_plugins


# -----------------------------------------------------------------------------
# Register a Sync filter class used for the 'sync' tasks
# -----------------------------------------------------------------------------

_PLUGIN_NAME = "diff_filters"


class DiffCollectionsFilter(ABC):
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
    def target_key_filter(item: dict):  # noqa
        """ return bool if the given item fields should be included in the key formation """
        return True

    @staticmethod
    def register(origin, target, collection, name="default"):
        def decorator(cls: DiffCollectionsFilter):
            cls.origin = origin
            cls.target = target
            cls.collection = collection
            key = (name, origin, target, collection)
            _registered_plugins[_PLUGIN_NAME][key] = cls
            return cls

        return decorator

    @staticmethod
    def get_registered(origin, target, collection, name="default"):
        key = (name, origin, target, collection)
        return _registered_plugins[_PLUGIN_NAME].get(key)


def default_filter(origin: Collection, target: Collection) -> DiffCollectionsFilter:
    apply_filter = DiffCollectionsFilter()
    apply_filter.fields = origin.FIELDS
    apply_filter.key_fields = origin.KEY_FIELDS
    apply_filter.collection = origin.name
    apply_filter.origin = origin.source.name
    apply_filter.target = target.source.name
    return apply_filter
