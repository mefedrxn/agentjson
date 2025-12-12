from __future__ import annotations

import json
from typing import Any, Optional, Tuple


def strict_parse(text: str) -> Tuple[bool, Optional[Any], Optional[json.JSONDecodeError]]:
    try:
        return True, json.loads(text), None
    except json.JSONDecodeError as e:
        return False, None, e
