#!/usr/bin/env python3
"""Demo script for json-prob-parser with complex broken JSON."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from json_prob_parser import parse, RepairOptions


def main():
    broken_file = Path(__file__).parent / "broken_complex.json"
    broken_text = broken_file.read_text()

    print("=" * 70)
    print("JSON Prob Parser - Complex Broken JSON Demo")
    print("=" * 70)
    print(f"\nInput file: {broken_file}")
    print(f"Input size: {len(broken_text)} bytes")
    print("\n" + "-" * 70)
    print("Issues in the broken JSON:")
    print("-" * 70)
    print("""
- Prefix/suffix garbage text ("Here is the API response:" / "End of response.")
- Markdown code fence wrapper
- JavaScript comments (// and /* */)
- Unquoted object keys throughout
- Single-quoted strings mixed with double-quoted
- Python literals: True, False, None
- Trailing commas in objects and arrays
- Missing commas between properties
- Unquoted array values: [admin, user, moderator]
- Nested objects with multiple issues
- Missing colons and brackets
""")

    print("-" * 70)
    print("Running parser with mode=probabilistic, beam_width=64, max_repairs=80...")
    print("-" * 70)

    result = parse(
        broken_text,
        RepairOptions(
            mode="probabilistic",
            top_k=3,
            beam_width=64,
            max_repairs=80,
            max_deleted_tokens=5,
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
        print(f"  - Confidence: {best.confidence:.6f}")
        print(f"  - Cost: {best.cost:.2f}")
        print(f"  - Repairs applied: {len(best.repairs)}")

        # Group repairs by type
        repair_types = {}
        for r in best.repairs:
            repair_types[r.op] = repair_types.get(r.op, 0) + 1

        print("\n" + "-" * 70)
        print("Repair operations summary:")
        print("-" * 70)
        for op, count in sorted(repair_types.items(), key=lambda x: -x[1]):
            print(f"  {op}: {count}")

        print("\n" + "-" * 70)
        print("First 25 repair operations:")
        print("-" * 70)
        for i, repair in enumerate(best.repairs[:25], 1):
            note = f" ({repair.note})" if repair.note else ""
            if repair.span:
                loc = f" at {repair.span}"
            elif repair.at:
                loc = f" at pos {repair.at}"
            else:
                loc = ""
            print(f"  {i:2}. {repair.op}{loc}{note}")

        if len(best.repairs) > 25:
            print(f"  ... and {len(best.repairs) - 25} more repairs")

        print("\n" + "-" * 70)
        print("Repaired JSON structure:")
        print("-" * 70)

        # Show structure overview
        if isinstance(best.value, dict):
            print(f"Root object with {len(best.value)} top-level keys:")
            for key in best.value.keys():
                val = best.value[key]
                if isinstance(val, dict):
                    print(f"  - {key}: object ({len(val)} keys)")
                elif isinstance(val, list):
                    print(f"  - {key}: array ({len(val)} items)")
                else:
                    print(f"  - {key}: {type(val).__name__}")

        print("\n" + "-" * 70)
        print("Sample of repaired data:")
        print("-" * 70)

        # Show sample data
        if best.value.get('database'):
            db = best.value['database']
            print(f"\nDatabase config:")
            print(f"  host: {db.get('host')}")
            print(f"  port: {db.get('port')}")
            if db.get('replicas'):
                print(f"  replicas: {len(db['replicas'])} configured")

        if best.value.get('features'):
            feat = best.value['features']
            print(f"\nFeature flags:")
            print(f"  dark_mode: {feat.get('enable_dark_mode')}")
            print(f"  notifications: {feat.get('enable_notifications')}")

        if best.value.get('users_data'):
            users = best.value['users_data']
            print(f"\nUsers: {len(users)} found")
            for u in users[:2]:
                print(f"  - {u.get('name', 'Unknown')}: {u.get('email', 'N/A')}")

        print("\n" + "-" * 70)
        print("Full repaired JSON:")
        print("-" * 70)
        print(json.dumps(best.value, indent=2, ensure_ascii=False))

    else:
        print("\nNo valid candidates found!")
        if result.errors:
            print("Errors:")
            for err in result.errors:
                print(f"  - {err.kind}: {err.message}")

    print("\n" + "=" * 70)
    print("Demo complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
