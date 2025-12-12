from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Mapping, Optional


_DEFAULT_SYSTEM = (
    "You are a JSON repair assistant.\n"
    "Return ONLY valid JSON. No markdown. No explanations.\n"
    "Prefer minimal local edits.\n"
)


def _extract_text_from_anthropic_message(resp: Any) -> str:
    """
    Best-effort extraction across Anthropic SDK response shapes.
    """
    # Newer SDK: resp.content = [{"type":"text","text":"..."}]
    content = getattr(resp, "content", None)
    if isinstance(content, list) and content:
        first = content[0]
        if isinstance(first, dict) and "text" in first:
            return str(first["text"])
        text = getattr(first, "text", None)
        if text is not None:
            return str(text)

    # Sometimes resp.output_text exists
    out_text = getattr(resp, "output_text", None)
    if out_text is not None:
        return str(out_text)

    # Fallback: stringify
    return str(resp)


@dataclass
class AnthropicPatchSuggestProvider:
    """
    Anthropic SDK provider that returns a patch_suggest JSON object.

    You must install the official SDK separately:
      pip install anthropic

    Env vars:
      - ANTHROPIC_API_KEY (recommended)
      - CLAUDE_MODEL or ANTHROPIC_MODEL
    """

    model: str = ""
    api_key: str = ""
    max_tokens: int = 1024
    temperature: float = 0.0
    system_prompt: str = _DEFAULT_SYSTEM

    def __post_init__(self) -> None:
        if not self.model:
            self.model = os.environ.get("CLAUDE_MODEL") or os.environ.get("ANTHROPIC_MODEL") or ""
        if not self.api_key:
            self.api_key = os.environ.get("ANTHROPIC_API_KEY") or ""
        if not self.model:
            # Leave empty but keep error message clear on call.
            self.model = ""

    def __call__(self, payload: Mapping[str, Any]) -> Any:
        try:
            from anthropic import Anthropic  # type: ignore[import-not-found]
        except Exception as e:  # noqa: BLE001
            raise RuntimeError("Anthropic SDK not installed. Run: pip install anthropic") from e

        if not self.api_key:
            raise RuntimeError("Missing ANTHROPIC_API_KEY (or pass api_key=...)")
        if not self.model:
            raise RuntimeError("Missing model (set CLAUDE_MODEL/ANTHROPIC_MODEL or pass model=...)")

        prompt = (
            "Return JSON in this format only:\n"
            "{\"mode\":\"patch_suggest\",\"patches\":[{\"patch_id\":\"p1\",\"ops\":[...],\"confidence\":0.5}]}\n"
            "\n"
            "PAYLOAD:\n"
            + json.dumps(payload, ensure_ascii=False)
        )

        client = Anthropic(api_key=self.api_key)
        resp = client.messages.create(
            model=self.model,
            max_tokens=int(self.max_tokens),
            temperature=float(self.temperature),
            system=self.system_prompt,
            messages=[{"role": "user", "content": prompt}],
        )
        text = _extract_text_from_anthropic_message(resp).strip()

        # Return parsed JSON if possible; otherwise return raw text.
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

