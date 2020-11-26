from nauti.collection import Collection
from nauti.log import get_logger
from nauti.diff import diff, DiffResults
from .filters import DiffCollectionsFilter, default_filter


async def diff_collections(
    origin_col: Collection,
    target_col: Collection,
    apply_filter: DiffCollectionsFilter,
    **options,
) -> DiffResults:  # noqa

    col_name = origin_col.name
    target_name = target_col.source.name
    origin_name = origin_col.source.name
    log = get_logger()

    if not apply_filter:
        apply_filter = default_filter(origin=origin_col, target=target_col)
        log.info(
            f"using default filter: fiels={apply_filter.fields}, keys={apply_filter.key_fields}"
        )

    log.info(f"Fetching {origin_name}/{col_name} collection ...")
    await origin_col.fetch(filters=apply_filter.origin_fetch_filter())
    log.info(
        f"Fetched {origin_name}/{col_name}, fetched {len(origin_col.source_records)} records."
    )

    log.info(f"Fetching {target_name}/{col_name} collection ...")
    await target_col.fetch(filters=apply_filter.target_fetch_filter())
    log.info(
        f"Fetched {target_name}/{col_name}, fetched {len(target_col.source_records)} records."
    )

    target_col.make_keys(
        *apply_filter.key_fields, with_filter=apply_filter.target_key_filter
    )
    origin_col.make_keys(
        *apply_filter.key_fields, with_filter=apply_filter.origin_key_filter
    )

    return diff(origin=origin_col, target=target_col, fields=apply_filter.fields)
