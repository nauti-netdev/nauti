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

import os
from typing import TextIO
from contextvars import ContextVar
from pathlib import Path

# -----------------------------------------------------------------------------
# Public Imports
# -----------------------------------------------------------------------------

import toml
from pydantic import ValidationError
from pydantic_env import config_validation_errors


# -----------------------------------------------------------------------------
# Private Imports
# -----------------------------------------------------------------------------

from .config_models import ConfigModel
from nauti import consts

# -----------------------------------------------------------------------------
# Exports
# -----------------------------------------------------------------------------


__all__ = ["get_config", "load_config_file", "load_default_config_file", "ConfigModel"]

# -----------------------------------------------------------------------------
#
#                              CODE BEGINS
#
# -----------------------------------------------------------------------------

g_config = ContextVar("config")


def get_config() -> ConfigModel:
    return g_config.get()


def load_config_file(filepath: TextIO):
    # as_fp = Path(filepath.name)
    # fp_dir = as_fp.parent
    # cfg_obj = dict()

    try:
        cfg_obj = toml.load(filepath)
        cfg_obj['config_file'] = filepath.name
    except ValueError as exc:
        raise RuntimeError(
            f"FAIL: loading file {str(exc)}"
        )

    # try:
    #     for each_fp in fp_dir.glob('*.toml'):
    #         cfg_obj.update(toml.load(each_fp.open()))
    # except Exception as exc:
    #     raise RuntimeError(f"FAIL: loading file {str(exc)}")

    try:
        config_obj = ConfigModel.parse_obj(cfg_obj)
        g_config.set(config_obj)
        return config_obj

    except ValidationError as exc:
        raise RuntimeError(
            config_validation_errors(errors=exc.errors(), filepath=filepath.name)
        )


def load_default_config_file():
    cfg_file = os.environ.get(consts.ENV_CONFIG_FILE, consts.DEFAULT_CONFIG_FILE)
    load_config_file(filepath=open(cfg_file))
