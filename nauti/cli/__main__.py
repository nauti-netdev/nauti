from importlib import metadata
import os
import logging

import click
import httpx
import uvloop
from parsimonious.exceptions import ParseError

from nauti.config import load_config_file
from nauti import consts
from nauti.log import setup_logging

VERSION = metadata.version("nauti")


@click.group()
@click.version_option(VERSION)
@click.option(
    "--config",
    "-C",
    type=click.File(),
    is_eager=True,
    default=lambda: os.environ.get(consts.ENV_CONFIG_FILE, consts.DEFAULT_CONFIG_FILE),
    callback=lambda ctx, param, value: load_config_file(filepath=value),
)
def cli(**kwargs):  # noqa
    """ Network automation tools integrator """
    pass


def main():
    uvloop.install()

    try:
        log = setup_logging()
        log.setLevel(logging.INFO)
        cli()

    except ParseError as exc:
        print(f"FAIL: Invalid filter expression: '{exc.text}'")

    except httpx.HTTPStatusError as exc:
        print(f"FAIL: HTTP error {exc.response.text}")

    except (httpx.ReadTimeout, httpx.PoolTimeout) as exc:
        print(f"FAIL: HTTP read timeout on URL: {exc.request.url}")
        print(f"BODY: {exc.request.stream._body}")  # noqa

    except RuntimeError as exc:
        print(exc.args[0])


if __name__ == "__main__":
    main()
