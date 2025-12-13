#!/usr/bin/env python3
"""
Generate large JSON demo files for agentjson.

By default this creates:
  - demo/huge_valid_array.json
  - demo/huge_broken_array.json  (missing final ']')

Usage:
  python3 demo/gen_huge_json.py --n 200000
"""

from __future__ import annotations

import argparse
from pathlib import Path


def _write_array(path: Path, n: int) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write("[")
        for i in range(n):
            if i:
                f.write(",")
            f.write('{"id":')
            f.write(str(i))
            f.write(',"name":"user')
            f.write(str(i))
            f.write('","active":true}')
        f.write("]")


def _make_truncated_array(valid_path: Path, broken_path: Path) -> None:
    data = valid_path.read_bytes()
    if not data.endswith(b"]"):
        raise ValueError("valid array must end with ']'")
    broken_path.write_bytes(data[:-1])


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=200_000, help="Number of objects to generate")
    p.add_argument("--out-dir", default=str(Path(__file__).parent), help="Output directory")
    p.add_argument("--prefix", default="huge", help="Output filename prefix")
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    valid = out_dir / f"{args.prefix}_valid_array.json"
    broken = out_dir / f"{args.prefix}_broken_array.json"

    _write_array(valid, args.n)
    _make_truncated_array(valid, broken)

    print(f"Wrote {valid} ({valid.stat().st_size} bytes)")
    print(f"Wrote {broken} ({broken.stat().st_size} bytes; missing closing ']')")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
