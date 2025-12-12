from __future__ import annotations

from typing import Any, Mapping, Optional


def _type_ok(v: Any, t: str) -> bool:
    if t == "int":
        return isinstance(v, int) and not isinstance(v, bool)
    if t == "float":
        return isinstance(v, (int, float)) and not isinstance(v, bool)
    if t == "str":
        return isinstance(v, str)
    if t == "bool":
        return isinstance(v, bool)
    if t == "object":
        return isinstance(v, dict)
    if t == "array":
        return isinstance(v, list)
    if t == "null":
        return v is None
    return True


def schema_match_score(value: Any, schema: Optional[Mapping[str, Any]]) -> Optional[float]:
    if schema is None:
        return None
    if not isinstance(value, dict):
        return 0.0

    required = schema.get("required_keys") or []
    types = schema.get("types") or {}

    req_ok = 1.0
    if required:
        present = sum(1 for k in required if k in value)
        req_ok = present / float(len(required))

    type_ok = 1.0
    if types:
        checks = 0
        good = 0
        for k, t in types.items():
            checks += 1
            if k in value and _type_ok(value[k], str(t)):
                good += 1
        type_ok = (good / float(checks)) if checks else 1.0

    return 0.5 * req_ok + 0.5 * type_ok
