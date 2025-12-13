"""orjson-compatible shim powered by agentjson.

This module is intended to be a *drop-in* replacement for `orjson` in
environments where the real `orjson` package is not installed.

Goals:
- Match the public surface area: `loads`, `dumps`, OPT_* flags, exceptions.
- Keep default behavior strict (like orjson), with optional env-controlled
  fallback to agentjson repair/scale logic.
"""

from __future__ import annotations

import dataclasses as _dataclasses
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime as _datetime
from datetime import time as _time
from datetime import timezone as _timezone
import json as _json
import os as _os
import uuid as _uuid
from typing import Any, Callable, Final, Optional, TypeVar, overload


__version__: Final[str] = "0.0.0+agentjson"


class JSONEncodeError(TypeError):
    """Raised when an object cannot be serialized to JSON."""


class JSONDecodeError(_json.JSONDecodeError):
    """Raised when input cannot be deserialized as JSON."""


T = TypeVar("T")


# ----------------------------
# Options (bitmask constants)
# ----------------------------

# NOTE: Values do not need to match upstream orjson as long as they are
# unique bit positions and can be OR'ed together.
OPT_APPEND_NEWLINE: Final[int] = 1 << 0
OPT_INDENT_2: Final[int] = 1 << 1
OPT_SORT_KEYS: Final[int] = 1 << 2
OPT_NON_STR_KEYS: Final[int] = 1 << 3

OPT_NAIVE_UTC: Final[int] = 1 << 4
OPT_UTC_Z: Final[int] = 1 << 5
OPT_OMIT_MICROSECONDS: Final[int] = 1 << 6
OPT_STRICT_INTEGER: Final[int] = 1 << 7

# Passthrough / ecosystem flags (present for compatibility; not implemented)
OPT_PASSTHROUGH_DATACLASS: Final[int] = 1 << 20
OPT_PASSTHROUGH_DATETIME: Final[int] = 1 << 21
OPT_PASSTHROUGH_SUBCLASS: Final[int] = 1 << 22
OPT_SERIALIZE_NUMPY: Final[int] = 1 << 23
OPT_SERIALIZE_DATACLASS: Final[int] = 1 << 24
OPT_SERIALIZE_UUID: Final[int] = 1 << 25


class Fragment:
    """Marker for raw JSON fragments.

    This is a minimal implementation compatible with `orjson.Fragment`:
    it stores already-serialized JSON bytes to be inserted verbatim by dumps().
    """

    __slots__ = ("_raw",)

    def __init__(self, raw: bytes) -> None:
        if not isinstance(raw, (bytes, bytearray, memoryview)):
            raise TypeError("Fragment must be initialized from bytes-like")
        self._raw = bytes(raw)

    @property
    def raw(self) -> bytes:
        return self._raw

    def __repr__(self) -> str:  # pragma: no cover
        return f"Fragment({self._raw!r})"


# ----------------------------
# Backend wiring
# ----------------------------

_HAVE_RUST = False
_rust = None

try:  # pragma: no cover - depends on local build
    from agentjson import agentjson_rust as _rust  # type: ignore[import-not-found]

    _HAVE_RUST = True
except Exception:  # noqa: BLE001
    _HAVE_RUST = False
    _rust = None


@dataclass(frozen=True)
class _ModeConfig:
    mode: str
    scale_threshold_bytes: int


def _read_mode_config() -> _ModeConfig:
    mode = _os.getenv("JSONPROB_ORJSON_MODE", "strict").strip().lower()
    threshold = int(_os.getenv("JSONPROB_ORJSON_SCALE_THRESHOLD_BYTES", "2000000"))
    return _ModeConfig(mode=mode, scale_threshold_bytes=threshold)


def _doc_for_decode_error(obj: Any) -> str:
    if isinstance(obj, str):
        return obj
    if isinstance(obj, (bytes, bytearray, memoryview)):
        try:
            return bytes(obj).decode("utf-8")
        except UnicodeDecodeError:
            return ""
    return ""


@overload
def loads(__obj: bytes | bytearray | memoryview | str) -> Any: ...


def loads(__obj: Any) -> Any:
    """Deserialize JSON (strict by default).

    In strict mode, this mirrors `orjson.loads`:
    - Requires the entire input be valid JSON with no leading/trailing junk.
    - Accepts bytes/bytearray/memoryview/str.
    - Raises JSONDecodeError on failure.

    Optional env-controlled fallback:
    - JSONPROB_ORJSON_MODE=auto: strict -> repair -> scale (for large root arrays)
    """

    if not isinstance(__obj, (bytes, bytearray, memoryview, str)):
        raise JSONDecodeError("input must be bytes, bytearray, memoryview, or str", "", 0)

    cfg = _read_mode_config()

    if _HAVE_RUST and _rust is not None:
        try:
            return _rust.strict_loads_py(__obj)  # type: ignore[attr-defined]
        except Exception as e:
            doc = _doc_for_decode_error(__obj)
            if cfg.mode not in ("auto", "repair"):
                raise _as_json_decode_error(e, doc) from e

            # repair/auto fallback (best-effort)
            try:
                import json_prob_parser as _jpp

                if (
                    isinstance(__obj, (bytes, bytearray, memoryview))
                    and doc == ""
                    and len(bytes(__obj)) > 0
                ):
                    raise ValueError("invalid utf-8")
                rr = _jpp.parse(doc, _jpp.RepairOptions(mode="probabilistic"))
                best = getattr(rr, "best", None)
                if best is not None and getattr(best, "value", None) is not None:
                    return best.value
            except Exception:
                pass

            if cfg.mode == "auto" and isinstance(__obj, (bytes, bytearray, memoryview)):
                b = bytes(__obj)
                if len(b) >= cfg.scale_threshold_bytes and b.lstrip().startswith(b"["):
                    try:
                        import json_prob_parser as _jpp

                        scale_res = _jpp.parse_root_array_scale(b, {"mode": "strict"})
                        return scale_res.get("value")
                    except Exception:
                        pass

            raise _as_json_decode_error(e, doc) from e

    # Pure-python strict fallback if Rust extension isn't available.
    if isinstance(__obj, str):
        doc = __obj
    else:
        try:
            doc = bytes(__obj).decode("utf-8")
        except UnicodeDecodeError as e:
            raise JSONDecodeError(
                "str is not valid UTF-8: surrogates not allowed",
                "",
                0,
            ) from e
    try:
        return _json.loads(doc)
    except _json.JSONDecodeError as e:
        raise JSONDecodeError(e.msg, e.doc, e.pos) from e


def _as_json_decode_error(err: Exception, doc: str) -> JSONDecodeError:
    args = getattr(err, "args", ())
    msg = args[0] if len(args) >= 1 else None
    if not isinstance(msg, str) or not msg:
        msg = str(err) if str(err) else "invalid JSON"

    pos = getattr(err, "pos", None)
    if not isinstance(pos, int):
        pos = args[1] if len(args) >= 2 else 0
    if not isinstance(pos, int):
        pos = 0
    return JSONDecodeError(msg, doc, pos)


def dumps(__obj: Any, default: Optional[Callable[[Any], Any]] = None, option: Optional[int] = None) -> bytes:
    """Serialize object to JSON bytes.

    Minimal drop-in implementation:
    - Uses Python's `json.dumps` but returns UTF-8 bytes (like orjson).
    - Supports OPT_INDENT_2, OPT_SORT_KEYS, OPT_APPEND_NEWLINE, OPT_NON_STR_KEYS,
      OPT_NAIVE_UTC, OPT_UTC_Z, OPT_OMIT_MICROSECONDS, OPT_STRICT_INTEGER.
    - Provides JSONEncodeError (TypeError subclass) for unsupported types.
    - Supports Fragment insertion via a safe placeholder + post-substitution.
    """

    if option is None:
        opt = 0
    elif isinstance(option, int):
        opt = option
    else:
        raise JSONEncodeError("option must be int")

    if opt & OPT_STRICT_INTEGER:
        _validate_strict_integers(__obj)

    obj = __obj
    if opt & OPT_NON_STR_KEYS:
        obj = _coerce_non_str_keys(obj)

    indent = 2 if (opt & OPT_INDENT_2) else None
    sort_keys = bool(opt & OPT_SORT_KEYS)
    separators = (",", ": ") if indent is not None else (",", ":")

    # Fragment support via placeholder substitution.
    fragments: list[bytes] = []

    def _default(o: Any) -> Any:
        if isinstance(o, Fragment):
            idx = len(fragments)
            fragments.append(o.raw)
            return f"__ORJSON_FRAGMENT_{idx}__"
        if _dataclasses.is_dataclass(o) and not isinstance(o, type):
            return _dataclasses.asdict(o)
        if isinstance(o, _datetime):
            return _format_datetime(o, opt)
        if isinstance(o, _date) and not isinstance(o, _datetime):
            return o.isoformat()
        if isinstance(o, _time):
            return o.isoformat()
        if isinstance(o, _uuid.UUID):
            return str(o)
        if default is not None:
            return default(o)
        raise TypeError(f"Type is not JSON serializable: {type(o).__name__}")

    try:
        text = _json.dumps(
            obj,
            default=_default,
            ensure_ascii=False,
            separators=separators,
            indent=indent,
            sort_keys=sort_keys,
        )
    except TypeError as e:
        raise JSONEncodeError(str(e)) from e

    out = text.encode("utf-8")

    if fragments:
        # Replace quoted placeholders with raw fragment bytes.
        for i, raw in enumerate(fragments):
            needle = _json.dumps(f"__ORJSON_FRAGMENT_{i}__", separators=(",", ":")).encode("utf-8")
            out = out.replace(needle, raw)

    if opt & OPT_APPEND_NEWLINE:
        out += b"\n"
    return out


def _format_datetime(dt: _datetime, opt: int) -> str:
    if opt & OPT_OMIT_MICROSECONDS:
        dt = dt.replace(microsecond=0)

    if dt.tzinfo is None:
        if opt & OPT_NAIVE_UTC:
            dt = dt.replace(tzinfo=_timezone.utc)
        return dt.isoformat()

    s = dt.isoformat()
    if opt & OPT_UTC_Z:
        try:
            if dt.utcoffset() == _timezone.utc.utcoffset(None):
                s = s.replace("+00:00", "Z")
        except Exception:
            pass
    return s


def _validate_strict_integers(obj: Any) -> None:
    limit = (1 << 53) - 1

    def walk(o: Any) -> None:
        if o is None or isinstance(o, bool):
            return
        if isinstance(o, int):
            if o > limit or o < -limit:
                raise JSONEncodeError("Integer exceeds 53-bit range")
            return
        if isinstance(o, (list, tuple)):
            for item in o:
                walk(item)
            return
        if isinstance(o, dict):
            for k, v in o.items():
                walk(k)
                walk(v)
            return

    walk(obj)


def _coerce_non_str_keys(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: dict[Any, Any] = {}
        for k, v in obj.items():
            if isinstance(k, (str, int, float, bool)) or k is None:
                kk: Any = k
            else:
                kk = str(k)
            out[kk] = _coerce_non_str_keys(v)
        return out
    if isinstance(obj, list):
        return [_coerce_non_str_keys(x) for x in obj]
    if isinstance(obj, tuple):
        return tuple(_coerce_non_str_keys(x) for x in obj)
    return obj
