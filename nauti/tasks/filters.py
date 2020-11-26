# -----------------------------------------------------------------------------
# System Imports
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# System Imports
# -----------------------------------------------------------------------------

from nauti.collection import Collection
from .registrar import _registered_plugins


# -----------------------------------------------------------------------------
# Register a Sync filter class used for the 'sync' tasks
# -----------------------------------------------------------------------------

_PLUGIN_NAME = "diff_filters"


class DiffCollectionsFilter(object):
    # The set of field names used to collect & sync
    fields = None

    # The set of field keys
    key_fields = None

    def __init__(self, origin: Collection, target: Collection):
        self.origin = origin
        self.target = target
        if not self.__class__.fields:
            self.fields = origin.FIELDS

        if not self.__class__.key_fields:
            self.key_fields = origin.KEY_FIELDS

        self.name = origin.name

    def origin_fetch_filter(self):
        """ returns a source specific fetch filter for the origin source """
        pass

    def origin_key_filter(self, item: dict):
        """ returns bool if the given item fields should be included in the key formation """
        return True

    def target_fetch_filter(self):
        """ returns a source specific fetch filter for the target source"""
        pass

    def target_key_filter(self, item: dict):  # noqa
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
    def get_registered(
        origin, target, collection, name="default"
    ) -> "DiffCollectionsFilter":
        key = (name, origin, target, collection)
        return _registered_plugins[_PLUGIN_NAME].get(key) or DiffCollectionsFilter
