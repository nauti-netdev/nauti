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

from typing import List, Dict, Any, Callable, Tuple, Optional, Type
from abc import ABC
from operator import itemgetter
from pkg_resources import iter_entry_points

# -----------------------------------------------------------------------------
# Public Imports
# -----------------------------------------------------------------------------

from bidict import bidict

# -----------------------------------------------------------------------------
# Private Imports
# -----------------------------------------------------------------------------

from nauti.log import get_logger
from nauti.source import Source
from nauti.entrypoints import NAUTI_EP_COLLECTIONS
from nauti.config import get_config

__all__ = ["Collection", "CollectionMixin", "CollectionCallback", "get_collection"]

CollectionCallback = Type[Callable[[Tuple, Any], None]]


class CollectionMixin(object):

    # The name of the collection, used when calling `get_collection`
    name = None

    # The source class bound to the collection, providing source-specific
    # collection implementations.
    source_class = None

    # List of expected fields collection provides
    FIELDS = None

    # Default set of field names used to create keys.
    KEY_FIELDS = None

    # List of additional fields that the collection makes avaiable.
    ADDNL_FIELDS = None

    # List of fields that collection does not provide, i.e. used to declare that
    # the collection provides a subset of FIELDS.
    NO_FIELDS = None


class Collection(ABC, CollectionMixin):

    # -------------------------------------------------------------------------
    #
    #                     Required Subclass Methods
    #
    # -------------------------------------------------------------------------

    async def fetch(self, **fetch_args):
        """ retrieve source records from the collection provider """
        raise NotImplementedError()

    # async def fetch_keys(self, keys: Dict):
    #     raise NotImplementedError()

    def itemize(self, rec: Dict) -> Dict:
        """ Translate a source specific record into a dict of field items """
        raise NotImplementedError()

    async def add_items(
        self, items: Dict, callback: Optional[CollectionCallback] = None
    ):
        """ add missing `items` to the collection """
        raise NotImplementedError()

    async def update_items(
        self, items: Dict, callback: Optional[CollectionCallback] = None
    ):
        """ update the existing `items` with field changes """
        raise NotImplementedError()

    async def delete_items(
        self, items: Dict, callback: Optional[CollectionCallback] = None
    ):
        """ remove the `items` from the collection """
        raise NotImplementedError()

    # -------------------------------------------------------------------------
    #
    #                     Class Init
    #
    # -------------------------------------------------------------------------

    def __init__(self, source: Source):

        # `source_records` is a list of recoreds as they are obtained from the
        # source.  The structure each source_records record is specific to the
        # source.  The inentory record is "fingerprinted" for collection fields;
        # which in turn are used to create inventory.

        self.source_records: List[Any] = list()

        # `inventory` is a dict where the key=<fields-key> and the value is the
        # fields-record of the source record.

        self.inventory: Dict[Tuple, Dict] = dict()

        # `source_record_keys` is a dict key=<fields-key>, value=<source-record>
        # that is used to cross reference the fields-key to a source specific
        # record ID which is typically found in the source specific response
        # record. The uid value is used when making updates to an exists record
        # in the source.

        self.source_record_keys: Dict[Tuple, Any] = dict()

        # The Source instance providing connectivity for the Collection
        # processing.

        self.source = source

        # `cache` is expected to be used by the subclass to store information
        # that it may need across various calls; for example caching device
        # records that may have information required by processing other
        # collections (ipaddrs).

        self.cache = dict()

        # `config` is the Config.collections[<name>] structure, initialied in
        # the call to get_collection().
        self.config = None

        # `mappings` ihe Config.collections[<name>].maps[<source-name>]
        # structure, initialized in the call to get_collection().
        self.maps = dict()

    def make_keys(
        self,
        *fields,
        with_filter: Optional[Callable[[Dict], bool]] = None,
        with_translate=None,
        with_inventory=None,
    ):
        if not len(self.source_records):
            get_logger().info(
                f"Collection {self.name}:{self.source_class.__name__}: inventory empty."
            )
            return

        with_filter = with_filter if with_filter else lambda x: True
        with_translate = with_translate or (lambda x: x)

        kf_getter = itemgetter(*(fields or self.KEY_FIELDS))

        if not with_inventory:
            self.inventory.clear()

        for rec in with_inventory or self.source_records:
            try:
                item = self.itemize(rec)

                if not with_filter(item):
                    continue

            except Exception as exc:
                raise RuntimeError("itimized failed", rec, exc)

            as_key = with_translate(kf_getter(item))
            self.inventory[as_key] = item
            self.source_record_keys[as_key] = rec

    # @classmethod
    # def get_collection(cls, source, name) -> "Collection":
    #     try:
    #         c_cls = next(
    #             iter(
    #                 c_cls
    #                 for c_cls in cls.__subclasses__()
    #                 if all(
    #                     (
    #                         c_cls.name == name,
    #                         c_cls.source_class,
    #                         isinstance(source, c_cls.source_class),
    #                     )
    #                 )
    #             )
    #         )
    #
    #     except StopIteration:
    #         raise RuntimeError(
    #             f"NOT-FOUND: Collection {name} for source class: {source.name}"
    #         )
    #
    #     return c_cls(source=source)

    def __len__(self):
        return len(self.source_records)


def get_collection(source, name: str) -> Collection:

    for ep in iter_entry_points(NAUTI_EP_COLLECTIONS, name):
        cls = ep.load()
        if isinstance(source, cls.source_class):
            break
    else:
        raise RuntimeError(f"ERROR:NOT-FOUND: nauti collection: {name}")

    cfg = get_config()
    col_obj: Collection = cls(source=source)
    col_obj.config = cfg.collections[name]

    if (col_maps := col_obj.config.maps.get(source.name)) is not None:
        for map_name, map_data in col_maps.items():
            col_obj.maps[map_name] = bidict(
                {key: value or key.lower() for key, value in map_data.items()}
            )

    return col_obj
