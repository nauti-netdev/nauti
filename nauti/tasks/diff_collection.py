from typing import Type

from nauti.collection import Collection
from nauti.log import get_logger
from nauti.diff import diff, DiffResults
from .filters import DiffCollectionsFilter


async def diff_collections(
    origin_col: Collection,
    target_col: Collection,
    diff_filter_cls: Type[DiffCollectionsFilter],
    **options,
) -> DiffResults:  # noqa

    col_name = origin_col.name
    target_name = target_col.source.name
    origin_name = origin_col.source.name
    log = get_logger()

    if not diff_filter_cls:
        diff_filter_cls = DiffCollectionsFilter
        log.info(
            f"using default filter: fiels={origin_col.FIELDS}, keys={origin_col.KEY_FIELDS}"
        )

    diff_filter = diff_filter_cls(origin=origin_col, target=target_col)
    if diff_filter.key_fields:
        origin_col.key_fields = target_col.key_fields = diff_filter.key_fields

    if diff_filter.fields:
        origin_col.fields = target_col.fields = diff_filter.fields

    # -------------------------------------------------------------------------
    # Obtain Origin Collections
    # -------------------------------------------------------------------------

    log.info(f"Fetching {origin_name}/{col_name} collection ...")
    orig_ff = options["origin_filter"] or diff_filter.origin_fetch_filter()
    await origin_col.fetch(filters=orig_ff)

    log.info(
        f"Fetched {origin_name}/{col_name}, fetched {len(origin_col.source_records)} records."
    )

    origin_col.make_keys(with_filter=diff_filter.origin_key_filter)

    # -------------------------------------------------------------------------
    # Obtain Target Collections
    # -------------------------------------------------------------------------

    target_ff = diff_filter.target_fetch_filter()

    log.info(f"Fetching {target_name}/{col_name} collection ...")

    await target_col.fetch(filters=target_ff)
    log.info(
        f"Fetched {target_name}/{col_name}, fetched {len(target_col.source_records)} records."
    )

    target_col.make_keys(with_filter=diff_filter.target_key_filter)

    return diff(origin=origin_col, target=target_col, fields=diff_filter.fields)
