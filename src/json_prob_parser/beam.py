from __future__ import annotations

import json
import math
from dataclasses import dataclass, replace
from typing import Iterable, Literal, Optional, Sequence, Tuple

from .lexer import Token, tolerant_lex
from .types import Candidate, CandidateDiagnostics, CandidateValidations, RepairAction, RepairOptions

ContainerType = Literal["OBJECT", "ARRAY"]

EXPECT_KEY_OR_END = "KEY_OR_END"
EXPECT_COLON = "COLON"
EXPECT_VALUE = "VALUE"
EXPECT_VALUE_OR_END = "VALUE_OR_END"
EXPECT_COMMA_OR_END = "COMMA_OR_END"

# Costs (initial defaults; tune with real data)
COST_REMOVE_TRAILING_COMMA = 0.2
COST_CLOSE_CONTAINER = 0.5
COST_INSERT_MISSING_COMMA = 0.8
COST_INSERT_MISSING_COLON = 1.0
COST_CONVERT_SINGLE_QUOTES = 0.9
COST_WRAP_KEY = 1.1
COST_WRAP_VALUE = 1.5
COST_SKIP_GARBAGE = 1.2
COST_DELETE_TOKEN = 2.5
COST_CLOSE_OPEN_STRING = 3.0
COST_TRUNCATE_SUFFIX = 1.3
COST_SYNTHESIZE_VALUE = 2.5
COST_PY_LITERAL_MAP = 0.4


@dataclass(frozen=True)
class Frame:
    typ: ContainerType
    expect: str


@dataclass(frozen=True)
class State:
    i: int
    stack: Tuple[Frame, ...]
    root_done: bool
    out: Tuple[str, ...]
    cost: float
    repairs: Tuple[RepairAction, ...]
    repair_count: int
    garbage_skipped_bytes: int
    deleted_tokens: int
    inserted_tokens: int
    close_open_string_count: int
    dropped_spans: Tuple[Tuple[int, int], ...]


def _top(state: State) -> Optional[Frame]:
    return state.stack[-1] if state.stack else None


def _set_top_expect(state: State, expect: str) -> State:
    if not state.stack:
        return state
    new_top = Frame(state.stack[-1].typ, expect)
    return replace(state, stack=state.stack[:-1] + (new_top,))


def _append_out(state: State, piece: str) -> State:
    return replace(state, out=state.out + (piece,))


def _pop_trailing_comma(state: State) -> Optional[State]:
    if not state.out:
        return None
    # We only emit commas as a standalone piece (",") in this implementation.
    if state.out[-1] != ",":
        return None
    return replace(state, out=state.out[:-1])


def _add_repair(
    state: State,
    *,
    op: str,
    cost_delta: float,
    span: Optional[Tuple[int, int]] = None,
    at: Optional[int] = None,
    token: Optional[str] = None,
    note: Optional[str] = None,
    inserted_tokens: int = 0,
    deleted_tokens: int = 0,
    garbage_skipped_bytes: int = 0,
    dropped_span: Optional[Tuple[int, int]] = None,
) -> Optional[State]:
    action = RepairAction(op=op, span=span, at=at, token=token, cost_delta=cost_delta, note=note)
    drops = state.dropped_spans
    if dropped_span is not None:
        drops = drops + (dropped_span,)
    close_open_inc = 1 if op == "close_open_string" else 0
    return replace(
        state,
        cost=state.cost + cost_delta,
        repairs=state.repairs + (action,),
        repair_count=state.repair_count + 1,
        inserted_tokens=state.inserted_tokens + inserted_tokens,
        deleted_tokens=state.deleted_tokens + deleted_tokens,
        garbage_skipped_bytes=state.garbage_skipped_bytes + garbage_skipped_bytes,
        close_open_string_count=state.close_open_string_count + close_open_inc,
        dropped_spans=drops,
    )


def _advance(state: State, n: int = 1) -> State:
    return replace(state, i=state.i + n)


def _is_value_start(token: Token) -> bool:
    if token.typ == "PUNCT" and token.value in ("{", "["):
        return True
    return token.typ in ("STRING", "NUMBER", "LITERAL", "IDENT")


def _is_key_start(token: Token) -> bool:
    if token.typ == "STRING":
        return True
    return token.typ in ("IDENT", "LITERAL")


def _complete_value_in_current_context(state: State) -> State:
    if not state.stack:
        return replace(state, root_done=True)
    top = state.stack[-1]
    if top.typ == "OBJECT" and top.expect == EXPECT_VALUE:
        return _set_top_expect(state, EXPECT_COMMA_OR_END)
    if top.typ == "ARRAY" and top.expect == EXPECT_VALUE_OR_END:
        return _set_top_expect(state, EXPECT_COMMA_OR_END)
    return state


def _consume_container_open(state: State, token: Token) -> Optional[State]:
    if token.typ != "PUNCT":
        return None
    if token.value == "{":
        state = _append_out(state, "{")
        state = replace(state, stack=state.stack + (Frame("OBJECT", EXPECT_KEY_OR_END),))
        return _advance(state)
    if token.value == "[":
        state = _append_out(state, "[")
        state = replace(state, stack=state.stack + (Frame("ARRAY", EXPECT_VALUE_OR_END),))
        return _advance(state)
    return None


def _consume_container_close(state: State, token: Token) -> Optional[State]:
    if token.typ != "PUNCT" or not state.stack:
        return None
    top = state.stack[-1]
    # If we are in KEY_OR_END/VALUE_OR_END and the output already ends with a comma,
    # accepting a close would produce an invalid trailing-comma JSON. Force a repair instead.
    if top.typ == "OBJECT" and top.expect == EXPECT_KEY_OR_END and state.out and state.out[-1] == ",":
        return None
    if top.typ == "ARRAY" and top.expect == EXPECT_VALUE_OR_END and state.out and state.out[-1] == ",":
        return None

    if top.typ == "OBJECT" and token.value == "}" and top.expect in (EXPECT_KEY_OR_END, EXPECT_COMMA_OR_END):
        state = _append_out(state, "}")
        state = replace(state, stack=state.stack[:-1])
        state = _advance(state)
        return _complete_value_in_current_context(state)
    if top.typ == "ARRAY" and token.value == "]" and top.expect in (EXPECT_VALUE_OR_END, EXPECT_COMMA_OR_END):
        state = _append_out(state, "]")
        state = replace(state, stack=state.stack[:-1])
        state = _advance(state)
        return _complete_value_in_current_context(state)
    return None


def _consume_punct(state: State, token: Token) -> Optional[State]:
    if token.typ != "PUNCT":
        return None

    # Root expects value-start; only { or [ are punct values.
    if not state.stack and not state.root_done:
        return _consume_container_open(state, token)

    top = _top(state)
    if top is None:
        return None

    # Container open as a value
    if top.expect in (EXPECT_VALUE, EXPECT_VALUE_OR_END) and token.value in ("{", "["):
        return _consume_container_open(state, token)

    # Close container
    closed = _consume_container_close(state, token)
    if closed is not None:
        return closed

    # Comma / Colon
    if token.value == "," and top.expect == EXPECT_COMMA_OR_END:
        state = _append_out(state, ",")
        if top.typ == "OBJECT":
            state = _set_top_expect(state, EXPECT_KEY_OR_END)
        else:
            state = _set_top_expect(state, EXPECT_VALUE_OR_END)
        return _advance(state)

    if token.value == ":" and top.typ == "OBJECT" and top.expect == EXPECT_COLON:
        state = _append_out(state, ":")
        state = _set_top_expect(state, EXPECT_VALUE)
        return _advance(state)

    return None


def _consume_key(state: State, token: Token, opt: RepairOptions) -> Optional[State]:
    top = _top(state)
    if top is None or top.typ != "OBJECT" or top.expect != EXPECT_KEY_OR_END:
        return None

    if token.typ == "STRING":
        piece = json.dumps(token.value, ensure_ascii=False)
        state2 = _append_out(state, piece)
        state2 = _advance(state2)
        state2 = _set_top_expect(state2, EXPECT_COLON)
        if token.quote == "'" and opt.allow_single_quotes:
            state2 = _add_repair(
                state2,
                op="convert_single_to_double_quotes",
                span=(token.start, token.end),
                cost_delta=COST_CONVERT_SINGLE_QUOTES,
            ) or state2
        if not token.closed:
            if state.close_open_string_count >= opt.max_close_open_string:
                return None
            state2 = _add_repair(
                state2,
                op="close_open_string",
                at=token.end,
                cost_delta=COST_CLOSE_OPEN_STRING,
            ) or state2
        return state2

    if token.typ in ("IDENT", "LITERAL") and opt.allow_unquoted_keys:
        piece = json.dumps(token.value, ensure_ascii=False)
        state2 = _append_out(state, piece)
        state2 = _advance(state2)
        state2 = _set_top_expect(state2, EXPECT_COLON)
        state2 = _add_repair(
            state2,
            op="wrap_key_with_quotes",
            span=(token.start, token.end),
            cost_delta=COST_WRAP_KEY,
        ) or state2
        return state2

    return None


def _consume_value_primitive(state: State, token: Token, opt: RepairOptions) -> Optional[State]:
    if not state.stack and not state.root_done:
        expect_value = True
    else:
        top = _top(state)
        expect_value = top is not None and top.expect in (EXPECT_VALUE, EXPECT_VALUE_OR_END)

    if not expect_value:
        return None

    if token.typ == "STRING":
        piece = json.dumps(token.value, ensure_ascii=False)
        state2 = _append_out(state, piece)
        state2 = _advance(state2)
        state2 = _complete_value_in_current_context(state2)
        if token.quote == "'" and opt.allow_single_quotes:
            state2 = _add_repair(
                state2,
                op="convert_single_to_double_quotes",
                span=(token.start, token.end),
                cost_delta=COST_CONVERT_SINGLE_QUOTES,
            ) or state2
        if not token.closed:
            if state.close_open_string_count >= opt.max_close_open_string:
                return None
            state2 = _add_repair(
                state2,
                op="close_open_string",
                at=token.end,
                cost_delta=COST_CLOSE_OPEN_STRING,
            ) or state2
        return state2

    if token.typ == "NUMBER":
        state2 = _append_out(state, token.value)
        state2 = _advance(state2)
        return _complete_value_in_current_context(state2)

    if token.typ == "LITERAL":
        state2 = _append_out(state, token.value.lower())
        state2 = _advance(state2)
        return _complete_value_in_current_context(state2)

    if token.typ == "IDENT":
        v = token.value
        low = v.lower()
        py_map = {"true": "true", "false": "false", "none": "null", "null": "null", "undefined": "null"}
        if opt.allow_python_literals and low in py_map:
            mapped = py_map[low]
            state2 = _append_out(state, mapped)
            state2 = _advance(state2)
            state2 = _complete_value_in_current_context(state2)
            if low not in ("true", "false", "null"):
                state2 = _add_repair(
                    state2,
                    op="map_python_literal",
                    span=(token.start, token.end),
                    cost_delta=COST_PY_LITERAL_MAP,
                    note=f"{v} -> {mapped}",
                ) or state2
            return state2

        if opt.allow_unquoted_values:
            piece = json.dumps(v, ensure_ascii=False)
            state2 = _append_out(state, piece)
            state2 = _advance(state2)
            state2 = _complete_value_in_current_context(state2)
            state2 = _add_repair(
                state2,
                op="wrap_value_with_quotes",
                span=(token.start, token.end),
                cost_delta=COST_WRAP_VALUE,
            ) or state2
            return state2

    return None


def _try_consume(state: State, token: Token, opt: RepairOptions) -> Optional[State]:
    if token.typ == "EOF":
        if not state.stack and state.root_done:
            return _advance(state)
        return None

    s = _consume_punct(state, token)
    if s is not None:
        return s
    s = _consume_key(state, token, opt)
    if s is not None:
        return s
    s = _consume_value_primitive(state, token, opt)
    if s is not None:
        return s
    return None


def _repair_remove_trailing_comma_before_end(state: State, token: Token, opt: RepairOptions) -> Optional[State]:
    if token.typ != "PUNCT" or token.value not in ("}", "]"):
        return None
    top = _top(state)
    if top is None:
        return None
    if top.typ == "OBJECT" and token.value == "}" and top.expect == EXPECT_KEY_OR_END:
        popped = _pop_trailing_comma(state)
        if popped is None:
            return None
        popped = _set_top_expect(popped, EXPECT_COMMA_OR_END)
        popped = _add_repair(
            popped,
            op="remove_trailing_comma",
            at=token.start,
            cost_delta=COST_REMOVE_TRAILING_COMMA,
        ) or popped
        return popped
    if top.typ == "ARRAY" and token.value == "]" and top.expect == EXPECT_VALUE_OR_END:
        popped = _pop_trailing_comma(state)
        if popped is None:
            return None
        popped = _set_top_expect(popped, EXPECT_COMMA_OR_END)
        popped = _add_repair(
            popped,
            op="remove_trailing_comma",
            at=token.start,
            cost_delta=COST_REMOVE_TRAILING_COMMA,
        ) or popped
        return popped
    return None


def _repair_insert_missing_comma(state: State, token: Token, opt: RepairOptions) -> Optional[State]:
    top = _top(state)
    if top is None or top.expect != EXPECT_COMMA_OR_END:
        return None
    if token.typ == "PUNCT" and token.value in ("}", "]"):
        return None

    # Context-weighted cost: clearer boundaries are cheaper.
    if token.typ == "STRING" or (token.typ == "PUNCT" and token.value in ("{", "[")):
        cost = 0.7
    elif token.typ == "IDENT":
        cost = 1.0
    else:
        cost = COST_INSERT_MISSING_COMMA

    if top.typ == "ARRAY" and _is_value_start(token):
        s = _append_out(state, ",")
        s = _set_top_expect(s, EXPECT_VALUE_OR_END)
        s = _add_repair(
            s,
            op="insert_missing_comma",
            at=token.start,
            token=",",
            cost_delta=cost,
            inserted_tokens=1,
        )
        return s

    if top.typ == "OBJECT" and _is_key_start(token):
        s = _append_out(state, ",")
        s = _set_top_expect(s, EXPECT_KEY_OR_END)
        s = _add_repair(
            s,
            op="insert_missing_comma",
            at=token.start,
            token=",",
            cost_delta=cost,
            inserted_tokens=1,
        )
        return s
    return None


def _repair_insert_missing_colon(state: State, token: Token, opt: RepairOptions) -> Optional[State]:
    top = _top(state)
    if top is None or top.typ != "OBJECT" or top.expect != EXPECT_COLON:
        return None
    if token.typ == "PUNCT" and token.value == ":":
        return None
    if _is_value_start(token) or (token.typ == "PUNCT" and token.value in ("{", "[")):
        s = _append_out(state, ":")
        s = _set_top_expect(s, EXPECT_VALUE)
        s = _add_repair(
            s,
            op="insert_missing_colon",
            at=token.start,
            token=":",
            cost_delta=COST_INSERT_MISSING_COLON,
            inserted_tokens=1,
        )
        return s
    return None


def _repair_skip_garbage(state: State, token: Token, opt: RepairOptions) -> Optional[State]:
    if token.typ not in ("GARBAGE",):
        return None
    tok_len = token.end - token.start
    if state.garbage_skipped_bytes + tok_len > opt.max_garbage_skip_bytes:
        return None
    cost = COST_SKIP_GARBAGE + (0.0002 * tok_len)
    s = _advance(state)
    s = _add_repair(
        s,
        op="skip_garbage",
        span=(token.start, token.end),
        cost_delta=cost,
        garbage_skipped_bytes=tok_len,
    )
    return s


def _repair_delete_unexpected(state: State, token: Token, opt: RepairOptions) -> Optional[State]:
    if token.typ == "EOF":
        return None
    if state.deleted_tokens >= opt.max_deleted_tokens:
        return None
    s = _advance(state)
    s = _add_repair(
        s,
        op="delete_unexpected_token",
        span=(token.start, token.end),
        cost_delta=COST_DELETE_TOKEN,
        deleted_tokens=1,
    )
    return s


def _repair_truncate_suffix(state: State, token: Token, *, text_len: int, eof_index: int) -> Optional[State]:
    if not state.out:
        return None
    if token.typ == "EOF":
        return None
    # Truncation is a "partial success" escape hatch; restrict it to clearly non-structural tokens
    # so we don't prematurely cut off valid JSON at commas/colons/brackets.
    if token.typ not in ("GARBAGE", "IDENT"):
        return None
    dropped = max(0, text_len - token.start)
    cost = COST_TRUNCATE_SUFFIX + (0.00005 * dropped)
    s = replace(state, i=eof_index)
    s = _add_repair(
        s,
        op="truncate_suffix",
        span=(token.start, text_len),
        cost_delta=cost,
        dropped_span=(token.start, text_len),
    )
    return s


def _repair_synthesize_missing_value(state: State, token: Token, opt: RepairOptions) -> Optional[State]:
    top = _top(state)
    expect_value = (not state.stack and not state.root_done) or (
        top is not None and top.expect in (EXPECT_VALUE, EXPECT_VALUE_OR_END)
    )
    if not expect_value:
        return None
    if token.typ == "EOF" or (token.typ == "PUNCT" and token.value in (",", "}", "]")):
        s = _append_out(state, "null")
        s = _add_repair(
            s,
            op="synthesize_missing_value",
            at=token.start,
            token="null",
            cost_delta=COST_SYNTHESIZE_VALUE,
            inserted_tokens=1,
        )
        s = _complete_value_in_current_context(s)
        return s
    return None


def _repair_close_one_container_at_eof(state: State, token: Token, opt: RepairOptions) -> Optional[State]:
    if token.typ != "EOF" or not state.stack:
        return None

    top = state.stack[-1]
    s = state

    if top.typ == "OBJECT" and top.expect == EXPECT_KEY_OR_END:
        popped = _pop_trailing_comma(s)
        if popped is not None:
            popped = _set_top_expect(popped, EXPECT_COMMA_OR_END)
            popped = _add_repair(
                popped,
                op="remove_trailing_comma",
                at=token.start,
                cost_delta=COST_REMOVE_TRAILING_COMMA,
            ) or popped
            s = popped
            top = s.stack[-1]

    if top.typ == "ARRAY" and top.expect == EXPECT_VALUE_OR_END:
        popped = _pop_trailing_comma(s)
        if popped is not None:
            popped = _set_top_expect(popped, EXPECT_COMMA_OR_END)
            popped = _add_repair(
                popped,
                op="remove_trailing_comma",
                at=token.start,
                cost_delta=COST_REMOVE_TRAILING_COMMA,
            ) or popped
            s = popped
            top = s.stack[-1]

    closer = "}" if top.typ == "OBJECT" else "]"
    s = _append_out(s, closer)
    s = replace(s, stack=s.stack[:-1])
    s = _add_repair(
        s,
        op="insert_missing_closer",
        at=token.start,
        token=closer,
        cost_delta=COST_CLOSE_CONTAINER,
        inserted_tokens=1,
    ) or s
    s = _complete_value_in_current_context(s)
    return s


def _expand_repairs(
    state: State,
    token: Token,
    opt: RepairOptions,
    *,
    text_len: int,
    eof_index: int,
    next_token: Optional[Token],
) -> list[State]:
    if state.repair_count >= opt.max_repairs:
        return []

    out: list[State] = []

    s = _repair_remove_trailing_comma_before_end(state, token, opt)
    if s is not None:
        out.append(s)

    s = _repair_insert_missing_comma(state, token, opt)
    if s is not None:
        out.append(s)

    s = _repair_insert_missing_colon(state, token, opt)
    if s is not None:
        out.append(s)

    s = _repair_synthesize_missing_value(state, token, opt)
    if s is not None:
        out.append(s)

    s = _repair_close_one_container_at_eof(state, token, opt)
    if s is not None:
        out.append(s)

    s = _repair_skip_garbage(state, token, opt)
    if s is not None:
        out.append(s)

    if opt.partial_ok:
        allow_truncate = True
        # Avoid truncating at an IDENT that is very likely a real (unquoted) key: IDENT followed by ':'.
        top = _top(state)
        if (
            token.typ == "IDENT"
            and top is not None
            and top.typ == "OBJECT"
            and top.expect == EXPECT_KEY_OR_END
            and next_token is not None
            and next_token.typ == "PUNCT"
            and next_token.value == ":"
        ):
            allow_truncate = False
        if allow_truncate:
            s = _repair_truncate_suffix(state, token, text_len=text_len, eof_index=eof_index)
            if s is not None:
                out.append(s)

    # Last resort: only if we haven't found a better idea.
    if not out:
        s = _repair_delete_unexpected(state, token, opt)
        if s is not None:
            out.append(s)

    return out


def _signature(state: State) -> tuple:
    tail = "".join(state.out[-8:])[-64:]
    return (state.i, state.root_done, state.stack, tail)


def _prune(states: Iterable[State], beam_width: int) -> list[State]:
    best: dict[tuple, State] = {}
    for s in states:
        sig = _signature(s)
        prev = best.get(sig)
        if prev is None or s.cost < prev.cost:
            best[sig] = s
    return sorted(best.values(), key=lambda s: (s.cost, s.repair_count, s.i))[:beam_width]


def _is_finished(state: State, token: Token) -> bool:
    return state.root_done and not state.stack and token.typ == "EOF"


def probabilistic_repair(
    extracted_text: str,
    opt: RepairOptions,
    *,
    base_repairs: Sequence[RepairAction] = (),
) -> list[Candidate]:
    tokens = tolerant_lex(extracted_text, allow_single_quotes=opt.allow_single_quotes)
    if not tokens:
        return []
    eof_index = len(tokens) - 1

    base_cost = sum(a.cost_delta for a in base_repairs)
    init = State(
        i=0,
        stack=(),
        root_done=False,
        out=(),
        cost=base_cost,
        repairs=tuple(base_repairs),
        repair_count=0,
        garbage_skipped_bytes=0,
        deleted_tokens=0,
        inserted_tokens=0,
        close_open_string_count=0,
        dropped_spans=(),
    )

    beam: list[State] = [init]
    finals: list[State] = []

    max_steps = max(64, len(tokens) * 4)
    for _ in range(max_steps):
        if not beam:
            break
        next_states: list[State] = []
        for s in beam:
            if s.i >= len(tokens):
                continue
            tok = tokens[s.i]
            if _is_finished(s, tok):
                finals.append(s)
                continue

            if s.root_done and not s.stack and tok.typ != "EOF":
                if tok.typ in ("GARBAGE", "IDENT"):
                    tok_len = tok.end - tok.start
                    if s.garbage_skipped_bytes + tok_len > opt.max_garbage_skip_bytes:
                        continue
                    cost = 0.3 + (0.0002 * tok_len)
                    s2 = _advance(s)
                    s2 = _add_repair(
                        s2,
                        op="skip_suffix",
                        span=(tok.start, tok.end),
                        cost_delta=cost,
                        garbage_skipped_bytes=tok_len,
                    ) or s2
                    next_states.append(s2)
                    continue

            consumed = _try_consume(s, tok, opt)
            if consumed is not None:
                next_states.append(consumed)
                strict_consume = (
                    consumed.cost == s.cost
                    and consumed.repair_count == s.repair_count
                    and len(consumed.repairs) == len(s.repairs)
                )
                if strict_consume:
                    continue

            next_tok = tokens[s.i + 1] if (s.i + 1) < len(tokens) else None
            next_states.extend(
                _expand_repairs(
                    s,
                    tok,
                    opt,
                    text_len=len(extracted_text),
                    eof_index=eof_index,
                    next_token=next_tok,
                )
            )

        beam = _prune(next_states, opt.beam_width)
        if len(finals) >= opt.top_k * 3:
            break

    candidates: list[Candidate] = []
    seen_norm: set[str] = set()
    for s in sorted(finals, key=lambda st: st.cost):
        norm = "".join(s.out).strip()
        if not norm:
            continue
        if norm in seen_norm:
            continue
        try:
            value = json.loads(norm)
        except json.JSONDecodeError:
            continue
        seen_norm.add(norm)
        cost = s.cost
        confidence = math.exp(-opt.confidence_alpha * cost)
        cand = Candidate(
            candidate_id=len(candidates),
            value=value,
            normalized_json=norm,
            ir=None,
            confidence=confidence,
            cost=cost,
            repairs=list(s.repairs),
            validations=CandidateValidations(strict_json_parse=True, schema_match=None),
            diagnostics=CandidateDiagnostics(
                garbage_skipped_bytes=s.garbage_skipped_bytes,
                deleted_tokens=s.deleted_tokens,
                inserted_tokens=s.inserted_tokens,
                close_open_string_count=s.close_open_string_count,
                beam_width=opt.beam_width,
                max_repairs=opt.max_repairs,
            ),
            dropped_spans=list(s.dropped_spans),
        )
        candidates.append(cand)
        if len(candidates) >= opt.top_k:
            break

    return candidates
