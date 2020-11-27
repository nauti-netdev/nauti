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
from functools import lru_cache

# -----------------------------------------------------------------------------
# Public Imports
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Private Imports
# -----------------------------------------------------------------------------

from nauti.log import get_logger
from nauti.source import Source
from nauti.config import get_config
from nauti.config_models import CollectionsModel

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
        """
        This method is used to fetch source records of information and append
        them into the `source_records` attribute.

        Other Parameters
        ----------------
        The fetch_args are specific to the underlying source API.  Meaning
        that the parameters provided here are going to be specific to how
        the source client filters/fetches records.
        """
        raise NotImplementedError()

    async def fetch_items(self, items: Dict):
        """
        This method is used to perform a bulk fetch of items based on the
        information provided in the `items` parameter. This method is typically
        called when the Caller needs to fetch a selection of items from one
        source based on the items in another.

        This subclass is expected to implement this method by making concurrent
        calls to the `fetch` method; thus ensuring a consisten behavior.

        Parameters
        ----------
        items: dict
            The dictionary of items to use as reference

        """
        raise NotImplementedError()

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

        # `fields` are the set of normalized fields used to create the items.  By
        # default these are taken from the class definition.  They can be
        # updated/changed during runtime.

        self.fields = self.__class__.FIELDS

        # `key_fields` are the set of fields used to create the item keys.  By
        # default these are taken from the class definition.  They can be
        # updated/changed during runtime.

        self.key_fields = self.__class__.KEY_FIELDS

        # `source_records` is a list of recoreds as they are obtained from the
        # source.  The structure each source_records record is specific to the
        # source.  The inentory record is "fingerprinted" for collection fields;
        # which in turn are used to create items.

        self.source_records: List[Any] = list()

        # `items` is a dict where the key=<fields-key> and the value is the
        # fields-record of the source record.

        self.items: Dict[Tuple, Dict] = dict()

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

        self.config: Optional[CollectionsModel] = None

    def make_keys(
        self,
        *key_fields,
        with_filter: Optional[Callable[[Dict], bool]] = None,
        with_translate=None,
        with_inventory=None,
    ):
        if not len(self.source_records):
            get_logger().info(
                f"Collection {self.name}:{self.source_class.__name__}: inventory empty."
            )
            return

        # if key_fields is provided, then update the instance attribute since
        # the Caller could invoke make_keys again without specifying key_fields
        # again.

        self.key_fields = key_fields or self.key_fields

        with_filter = with_filter if with_filter else lambda x: True
        with_translate = with_translate or (lambda x: x)

        kf_getter = itemgetter(*self.key_fields)

        if not with_inventory:
            self.items.clear()

        for rec in with_inventory or self.source_records:
            try:
                item = self.itemize(rec)

                if not with_filter(item):
                    continue

            except Exception as exc:
                import traceback

                raise RuntimeError(
                    f"Collection {self.name}: itimized failed.\n"
                    f"Record: {rec}\n"
                    f"Exception: {str(exc)}\n"
                    f"Traceback: {traceback.format_exc(limit=2)}"
                )

            as_key = with_translate(kf_getter(item))
            self.items[as_key] = item
            self.source_record_keys[as_key] = rec

    @lru_cache()
    def map_field_value(self, field, value):
        src_config = self.config.sources[self.source_class.name]
        mapped = src_config.maps.get(field, {}).get(value)
        return mapped or value

    @lru_cache()
    def imap_field_value(self, field, value):
        """
        Invert map a field value from normalized value to source specific value.

        Parameters
        ----------
        field: str
            The field name

        value: str
            The field value

        Returns
        -------
        Returns the mapped value if a mapping exists, or the original `value`.
        """

        if not (src_config := self.config.sources.get(self.source_class.name)):
            return value

        if not (field_bidict := src_config.maps.get(field)):
            return value

        mapped = field_bidict.inv.get(
            value
        )  # noqa: TODO need to triage why getting type inspection issue.
        return mapped or value

    def __len__(self):
        return len(self.source_records)


def get_collection(source: Source, name: str) -> Collection:

    if (
        cls := next(
            (
                cls
                for cls in Collection.__subclasses__()
                if cls.name == name and cls.source_class.name == source.name
            ),
            None,
        )
    ) is None:
        raise RuntimeError(f"ERROR:NOT-FOUND: nauti collection: {source.name}/{name}")

    cfg = get_config()
    col_obj: Collection = cls(source=source)
    col_obj.config = cfg.collections[name]
    return col_obj
