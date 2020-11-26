import click

opt_force_primary_ip = click.option(
    "--force-primary-ip",
    is_flag=True,
    help="When IP is already set, update IP if different",
)

opt_dry_run = click.option("--dry-run", is_flag=True, help="Dry-run mode")

opt_device_filter = click.option(
    "--filter", "filters", help="IPF device items filter expression",
)

opt_verbose = click.option("--verbose", "-v", help="detailed output", is_flag=True)


def csv_list(ctx, param, value):  # noqa
    return "" if not value else value.split(",")
