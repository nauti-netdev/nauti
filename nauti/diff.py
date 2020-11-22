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

from typing import Dict, Callable, Optional, Iterable
from tabulate import tabulate
from operator import itemgetter
from dataclasses import dataclass

from nauti.collection import Collection


@dataclass()
class DiffResults(object):
    source: Collection
    target: Collection
    missing: dict
    extras: dict
    changes: dict


def diff(
    source_from: Collection,
    sync_to: Collection,
    fields: Optional[Iterable] = None,
    fields_cmp: Optional[Dict[str, Callable]] = None,
):
    """
    The source and other collections have already been fetched, fingerprinted, and keyed.
    This method is used to create a diff report so that action can be taken to account for
    the differences.

    Parameters
    ----------
    source_from:
        The collection that is the source of truth for the diff

    sync_to:
        The collection that represents the destination of the update

    fields:
        A list of field names used for comparison purposes.  If not provided,
        then all fields in the collection FIELDS attribute will be used.  A
        Caller can provide `fields` when they want to diff only a subset of the
        collection FIELDS.

    fields_cmp:
        Dictionary mapping the field name to a function used to "normalize" the
        value so that it can be compared.  A common function would be
        `str.lower` to convert a field (hostname) to lower for comparison
        purposes.

    Returns
    -------
    DiffResults:
        missing: Dict[Tuple]
        changes: List[Tuple[Dict, Dict]]
    """
    sync_to_keys = set(sync_to.items)
    source_from_keys = set(source_from.items)

    missing_keys = source_from_keys - sync_to_keys
    extra_keys = sync_to_keys - source_from_keys
    shared_keys = source_from_keys & sync_to_keys

    # missing key dict; key=source_records-key, value=key-fingerprint
    missing_key_items = {key: source_from.items[key] for key in missing_keys}
    extra_key_items = {key: sync_to.items[key] for key in extra_keys}

    changes = dict()

    if not fields_cmp:
        fields_cmp = {}

    for field in fields or source_from.FIELDS:
        if field not in fields_cmp:
            fields_cmp[field] = lambda f: f

    for key in shared_keys:
        source_fp = source_from.items[key]
        sync_fp = sync_to.items[key]

        item_changes = dict()

        for field, field_fn in fields_cmp.items():
            if field_fn(source_fp[field]) != field_fn(sync_fp[field]):
                item_changes[field] = source_fp[field]

        if len(item_changes):
            changes[key] = item_changes

    if not any((missing_key_items, extra_key_items, changes)):
        return None

    return DiffResults(
        source=source_from,
        target=sync_to,
        missing=missing_key_items,
        changes=changes,
        extras=extra_key_items,
    )


def diff_report(diff_res, verbose: Optional[bool] = False):
    print("\nDiff Report")
    print(f"   Add items: count {len(diff_res.missing)}")
    print(f"   Remove items: count {len(diff_res.extras)}")
    print(f"   Update items: count {len(diff_res.changes)}")
    print("\n")

    diff_report_adds(diff_res.missing)
    diff_report_deletes(diff_res.extras)
    diff_report_updates(diff_res)


def diff_report_adds(items: dict):
    items = list(items.values())
    headers = items[0].keys()
    line = "-" * 80
    print(f"{line}\nAdd Items: {len(items)}\n{line}\n")
    print(
        tabulate(tabular_data=map(itemgetter(*headers), items), headers=headers),
        end="\n\n",
    )


def diff_report_deletes(items: dict):
    items = list(items.values())
    headers = items[0].keys()
    line = "-" * 80

    print(f"{line}\nRemove Items: {len(items)}\n{line}\n")

    print(
        tabulate(tabular_data=map(itemgetter(*headers), items), headers=headers),
        end="\n\n",
    )


def diff_report_updates(diff_res: DiffResults):
    changes = diff_res.changes
    fk = next(iter(changes))

    col = diff_res.target
    fields = col.items[fk].keys()

    def get_fields(rec):
        return [rec.get(field, "") for field in fields]

    line = "-" * 80

    print(f"{line}\nUpdate Items: {len(changes)}\n{line}\n")

    update_table = list()

    for key, upd_fields in changes.items():
        update_table.extend(get_fields(rec) for rec in [col.items[key], upd_fields, {}])

    print(tabulate(tabular_data=update_table, headers=fields))
