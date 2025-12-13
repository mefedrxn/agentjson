"""agentjson: distribution name + import alias for `json_prob_parser`.

This package exists so code can do:

    import agentjson

while preserving the original internal module name (`json_prob_parser`) for
backwards compatibility.

Also provides orjson-compatible API (loads, dumps, OPT_* flags) for drop-in usage.
"""

from __future__ import annotations

from json_prob_parser import *  # noqa: F403
from json_prob_parser import __all__ as _jpp_all

# orjson-compatible API
from orjson import (
    loads,
    dumps,
    JSONDecodeError,
    JSONEncodeError,
    Fragment,
    OPT_APPEND_NEWLINE,
    OPT_INDENT_2,
    OPT_SORT_KEYS,
    OPT_NON_STR_KEYS,
    OPT_NAIVE_UTC,
    OPT_UTC_Z,
    OPT_OMIT_MICROSECONDS,
    OPT_STRICT_INTEGER,
    OPT_PASSTHROUGH_DATACLASS,
    OPT_PASSTHROUGH_DATETIME,
    OPT_PASSTHROUGH_SUBCLASS,
    OPT_SERIALIZE_NUMPY,
    OPT_SERIALIZE_DATACLASS,
    OPT_SERIALIZE_UUID,
)

__all__ = [
    *_jpp_all,
    # orjson-compatible API
    "loads",
    "dumps",
    "JSONDecodeError",
    "JSONEncodeError",
    "Fragment",
    "OPT_APPEND_NEWLINE",
    "OPT_INDENT_2",
    "OPT_SORT_KEYS",
    "OPT_NON_STR_KEYS",
    "OPT_NAIVE_UTC",
    "OPT_UTC_Z",
    "OPT_OMIT_MICROSECONDS",
    "OPT_STRICT_INTEGER",
    "OPT_PASSTHROUGH_DATACLASS",
    "OPT_PASSTHROUGH_DATETIME",
    "OPT_PASSTHROUGH_SUBCLASS",
    "OPT_SERIALIZE_NUMPY",
    "OPT_SERIALIZE_DATACLASS",
    "OPT_SERIALIZE_UUID",
]
