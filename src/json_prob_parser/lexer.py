from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, List

TokenType = Literal["PUNCT", "STRING", "NUMBER", "LITERAL", "IDENT", "GARBAGE", "EOF"]


@dataclass(frozen=True)
class Token:
    typ: TokenType
    value: str
    start: int
    end: int
    quote: Optional[str] = None
    closed: bool = True


def _read_string(text: str, i: int, quote: str) -> tuple[Token, int]:
    start = i
    i += 1
    chars: list[str] = []
    escape = False
    while i < len(text):
        ch = text[i]
        if escape:
            if ch == "n":
                chars.append("\n")
            elif ch == "t":
                chars.append("\t")
            elif ch == "r":
                chars.append("\r")
            elif ch == "b":
                chars.append("\b")
            elif ch == "f":
                chars.append("\f")
            elif ch == "u" and i + 4 < len(text):
                hex_part = text[i + 1 : i + 5]
                try:
                    chars.append(chr(int(hex_part, 16)))
                    i += 4
                except ValueError:
                    chars.append("u")
            else:
                chars.append(ch)
            escape = False
            i += 1
            continue

        if ch == "\\":
            escape = True
            i += 1
            continue

        if ch == quote:
            return Token("STRING", "".join(chars), start, i + 1, quote=quote, closed=True), i + 1

        chars.append(ch)
        i += 1

    return Token("STRING", "".join(chars), start, len(text), quote=quote, closed=False), len(text)


def _read_number(text: str, i: int) -> tuple[Token, int]:
    start = i
    i += 1
    while i < len(text) and text[i].isdigit():
        i += 1
    if i < len(text) and text[i] == ".":
        i += 1
        while i < len(text) and text[i].isdigit():
            i += 1
    if i < len(text) and text[i] in ("e", "E"):
        i += 1
        if i < len(text) and text[i] in ("+", "-"):
            i += 1
        while i < len(text) and text[i].isdigit():
            i += 1
    return Token("NUMBER", text[start:i], start, i), i


def _read_word(text: str, i: int) -> tuple[Token, int]:
    start = i
    i += 1
    while i < len(text) and (text[i].isalnum() or text[i] == "_"):
        i += 1
    word = text[start:i]
    low = word.lower()
    if low in ("true", "false", "null"):
        return Token("LITERAL", low, start, i), i
    return Token("IDENT", word, start, i), i


def tolerant_lex(text: str, *, allow_single_quotes: bool = True) -> List[Token]:
    tokens: list[Token] = []
    i = 0
    while i < len(text):
        ch = text[i]
        if ch.isspace():
            i += 1
            continue
        if ch in "{}[],:":  # structural
            tokens.append(Token("PUNCT", ch, i, i + 1))
            i += 1
            continue
        if ch == '"':
            tok, i = _read_string(text, i, '"')
            tokens.append(tok)
            continue
        if ch == "'" and allow_single_quotes:
            tok, i = _read_string(text, i, "'")
            tokens.append(tok)
            continue
        if ch.isdigit() or ch == "-":
            tok, i = _read_number(text, i)
            tokens.append(tok)
            continue
        if ch.isalpha() or ch == "_":
            tok, i = _read_word(text, i)
            tokens.append(tok)
            continue

        # garbage chunk: read until whitespace or clear delimiter
        start = i
        i += 1
        while i < len(text) and (not text[i].isspace()) and text[i] not in "{}[],:\"'":
            i += 1
        tokens.append(Token("GARBAGE", text[start:i], start, i))

    tokens.append(Token("EOF", "", len(text), len(text)))
    return tokens
