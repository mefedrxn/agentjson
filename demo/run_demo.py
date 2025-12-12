#!/usr/bin/env python3
"""Demo script for json-prob-parser."""

import json
import sys
from pathlib import Path

# Add src to path for running without install
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from json_prob_parser import parse, RepairOptions


def main():
    # Read the broken JSON
    broken_file = Path(__file__).parent / "broken.json"
    broken_text = broken_file.read_text()

    print("=" * 60)
    print("JSON Prob Parser Demo")
    print("=" * 60)
    print(f"\nInput file: {broken_file}")
    print(f"Input size: {len(broken_text)} bytes")
    print("\n" + "-" * 60)
    print("Issues in the broken JSON:")
    print("-" * 60)
    print("""
- Markdown code fence wrapper (```json ... ```)
- Unquoted keys: name, api_config
- Single-quoted strings: 'age', 'dark', 'bob@example.com'
- Python literals: True, False (instead of true, false)
- Trailing commas: ["admin", "user",], "language": "en",
- Missing commas: "age": 25 "email" (missing comma between)
""")

    print("-" * 60)
    print("Running parser with mode=auto, top_k=3...")
    print("-" * 60)

    result = parse(
        broken_text,
        RepairOptions(
            mode="auto",
            top_k=3,
            beam_width=48,
            max_repairs=50,
            partial_ok=True,
            debug=False,
        ),
    )

    print(f"\nStatus: {result.status}")
    print(f"Mode used: {result.metrics.mode_used}")
    print(f"Elapsed: {result.metrics.elapsed_ms}ms")
    print(f"Candidates: {len(result.candidates)}")

    if result.candidates:
        best = result.candidates[0]
        print(f"\nBest candidate:")
        print(f"  - Confidence: {best.confidence:.4f}")
        print(f"  - Cost: {best.cost:.2f}")
        print(f"  - Repairs applied: {len(best.repairs)}")

        print("\n" + "-" * 60)
        print("Repair operations applied:")
        print("-" * 60)
        for i, repair in enumerate(best.repairs[:20], 1):
            note = f" ({repair.note})" if repair.note else ""
            span = f" at {repair.span}" if repair.span else (f" at pos {repair.at}" if repair.at else "")
            print(f"  {i:2}. {repair.op}{span}{note}")

        if len(best.repairs) > 20:
            print(f"  ... and {len(best.repairs) - 20} more repairs")

        print("\n" + "-" * 60)
        print("Repaired JSON (pretty-printed):")
        print("-" * 60)
        print(json.dumps(best.value, indent=2, ensure_ascii=False))

        # Validate the result
        print("\n" + "-" * 60)
        print("Validation:")
        print("-" * 60)
        print(f"  - Users count: {len(best.value.get('users', []))}")
        if best.value.get('users'):
            print(f"  - First user: {best.value['users'][0].get('name', 'N/A')}")
        if best.value.get('settings'):
            print(f"  - Settings theme: {best.value['settings'].get('theme', 'N/A')}")
        if best.value.get('statistics'):
            print(f"  - Total users: {best.value['statistics'].get('total_users', 'N/A')}")

    print("\n" + "=" * 60)
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
