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

from typing import Type
import asyncio
import click

from nauti.cli.__main__ import cli
from .cli_opts import opt_dry_run  # opt_verbose, csv_list
from nauti import tasks
from nauti.tasks.diff_collection import diff_collections
from nauti.diff import diff_report
from nauti.tasks.reconile import Reconciler
from nauti.tasks.diff_collection import DiffCollectionsFilter
from nauti.entrypoints import load_plugins


async def diff_sync(diff_task, reconciler_cls: Type[Reconciler], **options):
    diff_res = await diff_task(**options)

    diff_report(diff_res, reports=options.get("diff_report"))

    if options.get("dry_run") is True:
        return

    reconciler = reconciler_cls(diff_res=diff_res, **options)
    reco_opts = options["reconcile_action"]

    if diff_res.missing and any(("all" in reco_opts, "add" in reco_opts)):
        await reconciler.add_items()

    if diff_res.extras and any(("all" in reco_opts, "del" in reco_opts)):
        await reconciler.delete_items()

    if diff_res.changes and any(("all" in reco_opts, "upd" in reco_opts)):
        await reconciler.update_items()


@cli.command("sync")
@click.option("--spec", help="user-defined sync task name", default="default")
@click.option("--origin", help="origin source name", required=True)
@click.option("--target", help="target source name", required=True)
@click.option("--collection", help="collection name", required=True)
@click.option("--filter-name", help="user-defined sync filter name", default="default")
@click.option(
    "--diff-report",
    "--dr",
    type=click.Choice(["all", "add", "del", "upd"]),
    multiple=True,
)
@click.option(
    "--reconcile-action",
    "--ra",
    help="reconcile action(s)",
    type=click.Choice(["all", "add", "del", "upd"]),
    multiple=True,
)
@opt_dry_run
@click.pass_context
def cli_sync(ctx, origin, target, collection, **options):
    load_plugins()
    tasks.load_task_entrypoints()

    # ensure that there is a sync task registered for this
    # origin/target/collection, and if not exit with error.

    if (diff_task := tasks.get_diff_task(origin, target, collection)) is None:
        diff_task = diff_collections

    # see if the User specified an apply-filter by name.  If so then retrieve
    # that class definition from the registered list so that it can be passed to
    # the take as 'apply_filter' for use.

    options["apply_filter"] = DiffCollectionsFilter.get_registered(
        origin=origin, target=target, collection=collection, name=options["filter_name"]
    )

    if (
        reconcile_cls := Reconciler.get_registered(
            origin, target, collection, name="default"
        )
    ) is None:
        ctx.fail("ERROR: missing registered reconcile task")

    asyncio.run(diff_sync(diff_task=diff_task, reconciler_cls=reconcile_cls, **options))
