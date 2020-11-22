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
from .cli_opts import opt_dry_run, opt_verbose, csv_list
from nauti import tasks


@cli.command()
@click.option("--source", help="source name", required=True)
@click.option("--target", help="target sync name", required=True)
@click.option("--collection", help="collection name", required=True)
@click.option("--source-filter", help="filter expression applied to source collection")
@click.option("--target-filter", help="filter expression applied to target collection")
@click.option("--keys", help="comma separated list of key fields", callback=csv_list)
@opt_dry_run
@opt_verbose
@click.pass_context
def sync(ctx, source, target, collection, **options):
    tasks.load_task_entrypoints()
    sync_task = tasks.get_task("sync", source, target, collection)
    asyncio.run(sync_task(**options))
