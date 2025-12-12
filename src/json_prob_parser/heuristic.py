from __future__ import annotations

from typing import List, Tuple

from .types import RepairAction, RepairOptions


def _fix_smart_quotes(text: str) -> Tuple[str, List[RepairAction]]:
    # Common in copy/paste from docs or rich text.
    replacements = {
        "\u201c": '"',  # “
        "\u201d": '"',  # ”
        "\u2018": "'",  # ‘
        "\u2019": "'",  # ’
    }
    if not any(ch in text for ch in replacements):
        return text, []
    out = text.translate(str.maketrans(replacements))
    # Base cost only (we avoid per-char cost to keep v1 simple and deterministic).
    return out, [RepairAction(op="fix_smart_quotes", cost_delta=0.7)]


def _strip_comments(text: str) -> Tuple[str, List[RepairAction]]:
    out: list[str] = []
    repairs: list[RepairAction] = []
    i = 0
    in_string = False
    escape = False
    while i < len(text):
        ch = text[i]
        if in_string:
            out.append(ch)
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            i += 1
            continue

        if ch == '"':
            in_string = True
            out.append(ch)
            i += 1
            continue

        if ch == "/" and i + 1 < len(text) and text[i + 1] == "/":
            start = i
            i += 2
            while i < len(text) and text[i] not in "\r\n":
                i += 1
            repairs.append(RepairAction(op="strip_line_comment", span=(start, i), cost_delta=0.4))
            continue

        if ch == "/" and i + 1 < len(text) and text[i + 1] == "*":
            start = i
            i += 2
            while i + 1 < len(text) and not (text[i] == "*" and text[i + 1] == "/"):
                i += 1
            i = min(len(text), i + 2)
            repairs.append(RepairAction(op="strip_block_comment", span=(start, i), cost_delta=0.6))
            continue

        out.append(ch)
        i += 1

    return "".join(out), repairs


def _normalize_python_literals(text: str) -> Tuple[str, List[RepairAction]]:
    out: list[str] = []
    repairs: list[RepairAction] = []
    i = 0
    in_string = False
    escape = False
    mapping = {"True": "true", "False": "false", "None": "null", "undefined": "null"}
    while i < len(text):
        ch = text[i]
        if in_string:
            out.append(ch)
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            i += 1
            continue

        if ch == '"':
            in_string = True
            out.append(ch)
            i += 1
            continue

        if ch.isalpha() or ch == "_":
            start = i
            i += 1
            while i < len(text) and (text[i].isalnum() or text[i] == "_"):
                i += 1
            word = text[start:i]
            repl = mapping.get(word)
            if repl is not None:
                out.append(repl)
                repairs.append(
                    RepairAction(op="map_python_literal", span=(start, i), cost_delta=0.4, note=f"{word}->{repl}")
                )
            else:
                out.append(word)
            continue

        out.append(ch)
        i += 1

    return "".join(out), repairs


def _remove_trailing_commas(text: str) -> Tuple[str, List[RepairAction]]:
    out: list[str] = []
    repairs: list[RepairAction] = []
    i = 0
    in_string = False
    escape = False
    while i < len(text):
        ch = text[i]
        if in_string:
            out.append(ch)
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            i += 1
            continue

        if ch == '"':
            in_string = True
            out.append(ch)
            i += 1
            continue

        if ch == ",":
            j = i + 1
            while j < len(text) and text[j].isspace():
                j += 1
            if j >= len(text) or text[j] in ("}", "]"):
                repairs.append(RepairAction(op="remove_trailing_comma", at=i, cost_delta=0.2))
                i += 1
                continue

        out.append(ch)
        i += 1

    return "".join(out), repairs


def _append_missing_closers(text: str) -> Tuple[str, List[RepairAction]]:
    repairs: list[RepairAction] = []
    in_string = False
    escape = False
    depth_brace = 0
    depth_bracket = 0
    for ch in text:
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth_brace += 1
        elif ch == "}":
            depth_brace -= 1
        elif ch == "[":
            depth_bracket += 1
        elif ch == "]":
            depth_bracket -= 1

    out = text
    if in_string:
        out += '"'
        repairs.append(RepairAction(op="close_open_string", at=len(text), cost_delta=3.0))

    if depth_brace > 0 or depth_bracket > 0:
        out += ("]" * max(0, depth_bracket)) + ("}" * max(0, depth_brace))
        repairs.append(
            RepairAction(
                op="close_containers",
                at=len(text),
                cost_delta=0.5 * (max(0, depth_brace) + max(0, depth_bracket)),
                note=f"brace={depth_brace}, bracket={depth_bracket}",
            )
        )

    return out, repairs


def heuristic_repair(extracted_text: str, opt: RepairOptions) -> Tuple[str, List[RepairAction]]:
    text = extracted_text
    repairs: list[RepairAction] = []

    text2, acts = _fix_smart_quotes(text)
    if text2 != text:
        text = text2
        repairs.extend(acts)

    if opt.allow_comments:
        text2, acts = _strip_comments(text)
        if text2 != text:
            text = text2
            repairs.extend(acts)

    if opt.allow_python_literals:
        text2, acts = _normalize_python_literals(text)
        if text2 != text:
            text = text2
            repairs.extend(acts)

    text2, acts = _remove_trailing_commas(text)
    if text2 != text:
        text = text2
        repairs.extend(acts)

    text2, acts = _append_missing_closers(text)
    if text2 != text:
        text = text2
        repairs.extend(acts)

    return text, repairs
