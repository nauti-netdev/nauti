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

import asyncio
import click

from nauti.cli.__main__ import cli
from .cli_opts import opt_dry_run  # opt_verbose, csv_list
from nauti.diff import diff_report

from nauti.auditor import Auditor

from nauti.tasks.reconile import Reconciler
from nauti.log import get_logger


async def run_reconcile(reconciler):
    diff_res = reconciler.diff_res
    origin, target = diff_res.origin, diff_res.target
    sync_opts = reconciler.options

    async with origin.source, target.source:
        if diff_res.missing and any(("all" in sync_opts, "add" in sync_opts)):
            await reconciler.add_items()

        if diff_res.extras and any(("all" in sync_opts, "del" in sync_opts)):
            await reconciler.delete_items()

        if diff_res.changes and any(("all" in sync_opts, "upd" in sync_opts)):
            await reconciler.update_items()


async def run_audit(auditor: Auditor):
    async with auditor.origin.source, auditor.target.source:
        return await auditor.audit()


@cli.command("sync")
@click.option("--spec", help="user-defined sync task name", default="default")
@click.option("--origin", help="origin source name", required=True)
@click.option("--target", help="target source name", required=True)
@click.option("--collection", help="collection name", required=True)
@click.option("--filter-name", help="user-defined sync filter name", default="default")
@click.option("--origin-filter", help="origin specific filter expression")
@click.option(
    "--diff-report",
    "--dr",
    type=click.Choice(["all", "add", "del", "upd"]),
    multiple=True,
)
@click.option(
    "--sync-action",
    "--sync",
    help="reconcile action(s)",
    type=click.Choice(["all", "add", "del", "upd"]),
    multiple=True,
)
@opt_dry_run
@click.pass_context
def cli_sync(ctx, origin, target, collection, **options):

    # ensure that there is a sync task registered for this
    # origin/target/collection, and if not exit with error.

    auditor = Auditor.get_registered(
        origin=origin, target=target, collection=collection, name=options.get("auditor")
    )

    auditor.options = options

    # if (diff_task := tasks.get_diff_task(origin, target, collection)) is None:
    #     diff_task = diff_collections
    #
    # # see if the User specified an apply-filter by name.  If so then retrieve
    # # that class definition from the registered list so that it can be passed to
    # # the take as 'apply_filter' for use.
    #
    # options["diff_filter_cls"] = DiffCollectionsFilter.get_registered(
    #     origin=origin, target=target, collection=collection, name=options["filter_name"]
    # )

    loop = asyncio.get_event_loop()
    diff_res = loop.run_until_complete(run_audit(auditor))

    diff_report(diff_res, reports=options.get("diff_report"))

    if not (sync_opts := options["sync_action"]):
        return

    # Determine if there is a reconciler class registered for this combination.
    # if not, then report the error and exit.

    if (reconciler := Reconciler.get_registered(diff_res, name="default")) is None:
        get_logger().error("Missing registered reconcile task")
        return

    # Execute the sync reconcile process.

    reconciler.options = sync_opts
    loop.run_until_complete(run_reconcile(reconciler))

    # all done.
