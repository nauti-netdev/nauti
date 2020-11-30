from typing import Optional

from nauti.collection import Collection
from nauti.diff import DiffResults, diff
from nauti.log import get_logger
from nauti.tasks.registrar import _registered_plugins
from nauti.collection import get_collection
from nauti.source import get_source


_PLUGIN_NAME = "auditors"


class Auditor(object):
    # The name of the collection for this auditor
    name = None

    # The set of field names used to collect & sync
    fields = None

    # The set of field keys
    key_fields = None

    def __init__(self, origin: Collection, target: Collection, **options):
        self.name = origin.name

        self.origin = origin
        self.target = target
        self.diff_res: Optional[DiffResults] = None
        self.options = options

        if not self.__class__.fields:
            self.fields = origin.FIELDS

        if not self.__class__.key_fields:
            self.key_fields = origin.KEY_FIELDS

    async def fetch_origin(self):
        log = get_logger()

        ident = f"{self.origin.name}/{self.name}"
        log.info(f"Fetching {ident} collection ...")
        orig_ff = self.options.get("origin_filter") or self.origin_fetch_filter()
        await self.origin.fetch(filters=orig_ff)

        log.info(f"Fetched {ident}, fetched {len(self.origin.source_records)} records.")

        self.origin.make_keys(with_filter=self.origin_key_filter)

    async def fetch_target(self):
        log = get_logger()

        target_ff = self.options.get("target_filter") or self.target_fetch_filter()
        ident = f"{self.target.name}/{self.name}"

        log.info(f"Fetching {ident} collection ...")

        await self.target.fetch(filters=target_ff)
        log.info(f"Fetched {ident}, fetched {len(self.target.source_records)} records.")

        self.target.make_keys(with_filter=self.target_key_filter)

    async def audit(self) -> DiffResults:
        if self.key_fields:
            self.origin.key_fields = self.target.key_fields = self.key_fields

        if self.fields:
            self.origin.fields = self.target.fields = self.fields

        await self.fetch_origin()
        await self.fetch_target()
        self.diff_res = diff(origin=self.origin, target=self.target)
        return self.diff_res

    def origin_fetch_filter(self):  # noqa
        """ returns a source specific fetch filter for the origin source """
        return None

    def origin_key_filter(self, item: dict):  # noqa
        """ returns bool if the given item fields should be included in the key formation """
        return True

    def target_fetch_filter(self):  # noqa
        """ returns a source specific fetch filter for the target source"""
        return None

    def target_key_filter(self, item: dict):  # noqa
        """ return bool if the given item fields should be included in the key formation """
        return True

    @staticmethod
    def register(origin, target, collection, name=None):
        def decorator(cls: Auditor):
            key = (name or "default", origin, target, collection)
            _registered_plugins[_PLUGIN_NAME][key] = cls
            return cls

        return decorator

    @staticmethod
    def get_registered(
        origin: str, target: str, collection: str, name=None
    ) -> "Auditor":

        key = (name or "default", origin, target, collection)
        cls = _registered_plugins[_PLUGIN_NAME].get(key) or Auditor

        return cls(
            origin=get_collection(get_source(origin), collection),
            target=get_collection(get_source(target), collection),
        )
