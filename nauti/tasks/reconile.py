from nauti.diff import DiffResults
from .registrar import _registered_plugins

__all__ = ["Reconciler"]


_PLUGIN_NAME = "reconciler"


class Reconciler(object):
    def __init__(self, diff_res: DiffResults, **options):
        self.diff_res = diff_res
        self.options = options

        # The origin collection
        self.origin = diff_res.origin

        # The target collection
        self.target = diff_res.target

    async def add_items(self):
        pass

    async def delete_items(self):
        pass

    async def update_items(self):
        pass

    @staticmethod
    def register(origin, target, collection, name="default"):
        def decorator(cls):
            key = (name, origin, target, collection)
            _registered_plugins[_PLUGIN_NAME][key] = cls

        return decorator

    @staticmethod
    def get_registered(origin, target, collection, name="default"):
        key = (name, origin, target, collection)
        return _registered_plugins[_PLUGIN_NAME].get(key)
