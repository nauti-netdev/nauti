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
        raise NotImplementedError()

    async def delete_items(self):
        raise NotImplementedError()

    async def update_items(self):
        raise NotImplementedError()

    @staticmethod
    def register(origin, target, collection, name="default"):
        def decorator(cls):
            key = (name, origin, target, collection)
            _registered_plugins[_PLUGIN_NAME][key] = cls
            return cls

        return decorator

    @staticmethod
    def get_registered(diff_res: DiffResults, name="default") -> "Reconciler":
        key = (
            name,
            diff_res.origin.source.name,
            diff_res.target.source.name,
            diff_res.origin.name,
        )

        if not (cls := _registered_plugins[_PLUGIN_NAME].get(key)):
            return None

        return cls(diff_res)
