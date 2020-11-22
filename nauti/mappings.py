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

from typing import Callable
from functools import lru_cache, partial
import re

from nauti.config import get_config


@lru_cache()
def _expaner_os() -> Callable[[str], str]:
    config = get_config()
    cfg_map = config.maps["interfaces"]
    mapper = re.compile(r"|".join(list(cfg_map)))

    def _expander(ifname):
        return mapper.sub(lambda mo: cfg_map[mo.group(0)], ifname)

    return _expander


def expand_interface(ifname: str) -> str:
    return _expaner_os()(ifname)


@lru_cache()
def domain_remover():
    cfg_obj = get_config()
    any_domain = "|".join(
        re.escape(f".{domain}") for domain in cfg_obj.defaults.domain_names
    )
    return partial(re.compile(any_domain).sub, repl="")


def normalize_hostname(hostname):
    return domain_remover()(string=hostname.lower())
