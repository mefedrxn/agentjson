from __future__ import annotations

from typing import Any, Mapping

HAVE_RUST = False
_rust: Any = None

try:
    from agentjson import agentjson_rust as _rust  # type: ignore[import-not-found]

    HAVE_RUST = True
except Exception:  # noqa: BLE001
    HAVE_RUST = False
    _rust = None


def _missing_rust_error() -> RuntimeError:
    return RuntimeError(
        "Rust backend not installed. Build/install the PyO3 extension:\n"
        "  python -m pip install -U maturin\n"
        "  maturin develop\n"
    )


def parse(input_text_or_bytes: Any, options: Mapping[str, Any]) -> dict:
    if not HAVE_RUST or _rust is None:
        raise _missing_rust_error()
    return _rust.parse_py(input_text_or_bytes, dict(options))


def preprocess(input_text_or_bytes: Any, options: Mapping[str, Any]) -> dict:
    if not HAVE_RUST or _rust is None:
        raise _missing_rust_error()
    return _rust.preprocess_py(input_text_or_bytes, dict(options))


def probabilistic_repair(extracted_text: str, options: Mapping[str, Any], base_repairs: list[dict]) -> list[dict]:
    if not HAVE_RUST or _rust is None:
        raise _missing_rust_error()
    return _rust.probabilistic_repair_py(extracted_text, dict(options), list(base_repairs))


def parse_root_array_scale(data: bytes, options: Mapping[str, Any]) -> dict:
    if not HAVE_RUST or _rust is None:
        raise _missing_rust_error()
    return _rust.parse_root_array_scale_py(data, dict(options))
