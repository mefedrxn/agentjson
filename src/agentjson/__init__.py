"""agentjson: PyPI distribution name and import alias.

This repo historically exposed its Python API as `json_prob_parser`. We keep that
internal module name for backwards compatibility, while allowing new code to:

    import agentjson

This module is intentionally *thin* and uses lazy forwarding to avoid circular
imports with the bundled `orjson` shim and the Rust extension module.

Drop-in patterns:
    import agentjson as orjson  # keep callsites like orjson.loads/dumps
    import orjson               # use the bundled shim (if real orjson is absent)
"""

from __future__ import annotations

from typing import Any, Final


_ORJSON_EXPORTS: Final[set[str]] = {
    # Public functions
    "loads",
    "dumps",
    # Exceptions
    "JSONDecodeError",
    "JSONEncodeError",
    # Helpers
    "Fragment",
    # Options
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
}


def __getattr__(name: str) -> Any:  # pragma: no cover
    # Allow importing the bundled Rust extension without triggering lazy
    # forwarding (which would recurse back into `json_prob_parser` during init).
    if name == "agentjson_rust":
        import importlib

        return importlib.import_module("agentjson.agentjson_rust")

    if name in _ORJSON_EXPORTS:
        import orjson as _orjson

        return getattr(_orjson, name)

    import json_prob_parser as _jpp

    try:
        return getattr(_jpp, name)
    except AttributeError as e:
        raise AttributeError(f"module 'agentjson' has no attribute {name!r}") from e


def __dir__() -> list[str]:  # pragma: no cover
    try:
        import json_prob_parser as _jpp

        jpp_names = set(dir(_jpp))
    except Exception:
        jpp_names = set()

    return sorted(jpp_names | set(_ORJSON_EXPORTS))
