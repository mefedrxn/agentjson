from __future__ import annotations

import json
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from multiprocessing.shared_memory import SharedMemory
from typing import Any, Iterable, Iterator, Literal, Optional, Sequence, Tuple

from .types import RepairOptions


SplitMode = Literal["NO_SPLIT", "ROOT_ARRAY_ELEMENTS"]


@dataclass(frozen=True)
class SplitPlan:
    mode: SplitMode
    elements: int
    structural_density: float
    chunk_count: int


def _is_ws(b: int) -> bool:
    return b in (9, 10, 13, 32)  # \t \n \r space


def _trim_ws(data: bytes) -> tuple[int, int]:
    start = 0
    end = len(data)
    # UTF-8 BOM
    if end >= 3 and data[:3] == b"\xef\xbb\xbf":
        start = 3
    while start < end and _is_ws(data[start]):
        start += 1
    while end > start and _is_ws(data[end - 1]):
        end -= 1
    return start, end


def _iter_root_array_element_spans(data: bytes, start: int, end: int) -> Iterator[Tuple[int, int]]:
    """
    Yield (start,end) byte spans for each element in a root JSON array.
    `start` points at '[' and `end` points just past ']' (exclusive).
    """
    if start >= end or data[start] != ord("[") or data[end - 1] != ord("]"):
        return

    i = start + 1
    while i < end and _is_ws(data[i]):
        i += 1
    if i >= end - 1:
        return  # empty array

    elem_start = i
    in_string = False
    escape = False
    depth_brace = 0
    depth_bracket = 1  # root '[' already entered

    for i in range(start + 1, end - 1):
        ch = data[i]
        if in_string:
            if escape:
                escape = False
            elif ch == 92:  # backslash
                escape = True
            elif ch == 34:  # quote
                in_string = False
            continue

        if ch == 34:  # quote
            in_string = True
            continue

        if ch == 123:  # {
            depth_brace += 1
            continue
        if ch == 125:  # }
            depth_brace -= 1
            continue
        if ch == 91:  # [
            depth_bracket += 1
            continue
        if ch == 93:  # ]
            depth_bracket -= 1
            continue

        if ch == 44 and depth_brace == 0 and depth_bracket == 1:  # comma at depth==1
            elem_end = i
            # trim whitespace around element
            s = elem_start
            e = elem_end
            while s < e and _is_ws(data[s]):
                s += 1
            while e > s and _is_ws(data[e - 1]):
                e -= 1
            if e > s:
                yield (s, e)
            elem_start = i + 1

    # last element
    s = elem_start
    e = end - 1
    while s < e and _is_ws(data[s]):
        s += 1
    while e > s and _is_ws(data[e - 1]):
        e -= 1
    if e > s:
        yield (s, e)


def _root_array_split_plan(data: bytes, start: int, end: int, opt: RepairOptions) -> tuple[SplitPlan, list[list[Tuple[int, int]]]]:
    # Single pass: collect spans + structural stats.
    spans = list(_iter_root_array_element_spans(data, start, end))
    elements = len(spans)

    # Structural density approximation (outside strings is expensive to recompute);
    # use "elements count + brackets" as a cheap proxy plus a small scan for delimiters.
    structural = 0
    in_string = False
    escape = False
    for i in range(start, end):
        ch = data[i]
        if in_string:
            if escape:
                escape = False
            elif ch == 92:
                escape = True
            elif ch == 34:
                in_string = False
            continue
        if ch == 34:
            in_string = True
            continue
        if ch in (123, 125, 91, 93, 44, 58):  # {}[] , :
            structural += 1
    structural_density = structural / max(1, (end - start))

    allow_parallel = opt.allow_parallel
    if allow_parallel == "auto":
        do_parallel = (
            (end - start) >= opt.parallel_threshold_bytes
            and elements >= opt.min_elements_for_parallel
            and structural_density >= opt.density_threshold
        )
    else:
        do_parallel = bool(allow_parallel)

    if not do_parallel or elements <= 1:
        return SplitPlan(mode="NO_SPLIT", elements=elements, structural_density=structural_density, chunk_count=1), [
            spans
        ]

    target = max(1_000_000, int(opt.parallel_chunk_bytes))
    tasks: list[list[Tuple[int, int]]] = []
    cur: list[Tuple[int, int]] = []
    cur_bytes = 0
    for s, e in spans:
        cur.append((s, e))
        cur_bytes += (e - s)
        if cur and cur_bytes >= target:
            tasks.append(cur)
            cur = []
            cur_bytes = 0
    if cur:
        tasks.append(cur)

    return (
        SplitPlan(mode="ROOT_ARRAY_ELEMENTS", elements=elements, structural_density=structural_density, chunk_count=len(tasks)),
        tasks,
    )


def _parse_task_bytes(data: bytes, spans: Sequence[Tuple[int, int]]) -> list[Any]:
    parts = [data[s:e] for s, e in spans]
    # Build a mini array to reduce parser overhead (one json.loads per task).
    payload = b"[" + b",".join(parts) + b"]"
    return json.loads(payload)


_WORKER_BUF = None  # type: ignore[var-annotated]
_WORKER_SHM = None  # type: ignore[var-annotated]


def _worker_init(shm_name: str) -> None:
    global _WORKER_BUF, _WORKER_SHM
    shm = SharedMemory(name=shm_name)
    _WORKER_SHM = shm
    _WORKER_BUF = shm.buf


def _worker_parse_task(spans: Sequence[Tuple[int, int]]) -> list[Any]:
    global _WORKER_BUF
    assert _WORKER_BUF is not None
    parts = [bytes(_WORKER_BUF[s:e]) for s, e in spans]
    payload = b"[" + b",".join(parts) + b"]"
    return json.loads(payload)


def parse_root_array_scale(data: bytes, opt: RepairOptions) -> tuple[list[Any], SplitPlan]:
    """
    Scale path: strict parsing for root arrays, with optional safe-split parallelism.
    Returns (elements, split_plan).
    """
    s0, e0 = _trim_ws(data)
    if e0 - s0 <= 2 or data[s0] != ord("[") or data[e0 - 1] != ord("]"):
        # Not a root array; let the caller decide what to do.
        value = json.loads(data[s0:e0])
        return value, SplitPlan(mode="NO_SPLIT", elements=0, structural_density=0.0, chunk_count=1)

    plan, tasks = _root_array_split_plan(data, s0, e0, opt)
    if plan.mode == "NO_SPLIT":
        return _parse_task_bytes(data, tasks[0]), plan

    workers = int(opt.parallel_workers or (os.cpu_count() or 2))
    workers = max(1, workers)

    if opt.parallel_backend == "thread":
        with ThreadPoolExecutor(max_workers=workers) as ex:
            out: list[Any] = []
            for chunk in ex.map(lambda spans: _parse_task_bytes(data, spans), tasks):
                out.extend(chunk)
            return out, plan

    shm = SharedMemory(create=True, size=len(data))
    try:
        shm.buf[: len(data)] = data
        with ProcessPoolExecutor(max_workers=workers, initializer=_worker_init, initargs=(shm.name,)) as ex:
            out: list[Any] = []
            for chunk in ex.map(_worker_parse_task, tasks):
                out.extend(chunk)
            return out, plan
    finally:
        shm.close()
        shm.unlink()

