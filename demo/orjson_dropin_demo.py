#!/usr/bin/env python3
"""Demo: use `agentjson` as an `orjson`-compatible drop-in.

Run:
  python demo/orjson_dropin_demo.py

Notes:
  - This demo aliases `agentjson` as `orjson` so the call sites look identical.
  - The orjson-compatible API stays strict by default; enable repair/scale fallback with:
      export JSONPROB_ORJSON_MODE=auto
"""

from __future__ import annotations

import os

import agentjson as orjson


def _try_load(label: str, payload: str) -> None:
    try:
        v = orjson.loads(payload)
        print(f"[{label}] ok:", v)
    except Exception as e:  # noqa: BLE001
        print(f"[{label}] err:", type(e).__name__, str(e).splitlines()[0])


def main() -> None:
    messy = 'preface```json\\n{\"a\": 1,}\\n```suffix'

    print("module:", orjson.__name__)
    print()

    os.environ["JSONPROB_ORJSON_MODE"] = "strict"
    _try_load("strict", messy)

    os.environ["JSONPROB_ORJSON_MODE"] = "auto"
    _try_load("auto", messy)

    blob = orjson.dumps({"a": 1}, option=(orjson.OPT_APPEND_NEWLINE | orjson.OPT_SORT_KEYS))
    print()
    print("dumps ->", blob)


if __name__ == "__main__":
    main()
