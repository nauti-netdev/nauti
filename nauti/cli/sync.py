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
from .cli_opts import opt_dry_run, opt_verbose  # csv_list
from nauti import tasks


@cli.command()
@click.option("--origin", help="origin source name", required=True)
@click.option("--target", help="target source name", required=True)
@click.option("--collection", help="collection name", required=True)
@click.option("--apply-filter", help="name of registered sync filter")
@opt_dry_run
@opt_verbose
@click.pass_context
def sync(ctx, origin, target, collection, **options):
    tasks.load_task_entrypoints()
    sync_task = tasks.get_task("sync", origin, target, collection)
    if not sync_task:
        ctx.fail("Sync task does not exist")

    if (apply_filter_name := options.get("apply_filter")) is not None:
        if not (
            apply_filter := tasks.get_filter(
                "sync",
                origin=origin,
                target=target,
                collection=collection,
                name=apply_filter_name,
            )
        ):
            ctx.fail(f"NOT-FOUND: apply-filter: {apply_filter_name}")

        options["apply_filter"] = apply_filter

    asyncio.run(sync_task(**options))
