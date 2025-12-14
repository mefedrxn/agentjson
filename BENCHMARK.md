# Benchmarks (agentjson)

This document explains what `benchmarks/bench.py` measures and **why** the suites are structured the way they are, based on the Slack thread:

- “LLM outputs sometimes break JSON or wrap it (‘json입니다~ …’, markdown fences, etc.)”
- “Huge JSON (GB-scale root arrays / big data) might want parallel parsing”
- “A probabilistic parser that can return multiple candidates (Top‑K) is valuable”

> TL;DR: `agentjson` is not trying to beat `orjson` on valid JSON microbenchmarks. The primary win is **success rate** and **drop-in adoption** in LLM pipelines.

## How to run

Because `agentjson` provides a top-level `orjson` drop-in module, you must use **two separate environments** if you want to compare with the real `orjson` package:

```bash
# Env A: real orjson
python -m venv .venv-orjson
source .venv-orjson/bin/activate
python -m pip install orjson ujson
python benchmarks/bench.py

# Env B: agentjson (includes the shim)
python -m venv .venv-agentjson
source .venv-agentjson/bin/activate
python -m pip install agentjson ujson
python benchmarks/bench.py
```

Tune run sizes with env vars:

```bash
BENCH_MICRO_NUMBER=20000 BENCH_MICRO_REPEAT=5 \
BENCH_MESSY_NUMBER=2000 BENCH_MESSY_REPEAT=5 \
BENCH_TOPK_NUMBER=500 BENCH_TOPK_REPEAT=5 \
BENCH_LARGE_MB=5,20 BENCH_LARGE_NUMBER=3 BENCH_LARGE_REPEAT=3 \
BENCH_NESTED_MB=5,20 BENCH_NESTED_NUMBER=1 BENCH_NESTED_REPEAT=3 \
BENCH_NESTED_FORCE_PARALLEL=0 \
BENCH_CLI_MMAP_MB=512 \
python benchmarks/bench.py
```

### Mapping suites to PRs (006 / 101 / 102)

- **PR‑006 (CLI mmap)**: run `cli_mmap_suite` by setting `BENCH_CLI_MMAP_MB`.
  - This runs the Rust CLI twice on the same large file: default mmap vs `--no-mmap`.
  - It uses `/usr/bin/time` when available to capture max RSS.
- **PR‑101 (parallel delimiter indexer)**: use `large_root_array_suite` and increase `BENCH_LARGE_MB` (e.g. `200,1000`) to find the crossover where parallel indexing starts paying off.
- **PR‑102 (nested huge value / corpus)**: use `nested_corpus_suite` to benchmark `scale_target_keys=["corpus"]` with `allow_parallel` on/off.

### Example: CLI mmap suite (PR‑006)

On macOS this suite records **wall time** (the `/usr/bin/time -v` max-RSS path is Linux-friendly).

Example run (Env B, `BENCH_CLI_MMAP_MB=256`, 2025-12-14):

| Mode | Elapsed |
|---|---:|
| `mmap(default)` | 1.27 s |
| `read(--no-mmap)` | 1.38 s |

Interpretation: mmap’s main win is avoiding **upfront heap allocation / extra copies** on huge files; it may or may not be faster depending on OS page cache and IO patterns.

## Suite 1 — LLM messy JSON suite (primary)

### What it tests

Real LLM outputs often include things that break strict JSON parsers:

- Markdown fences: ```` ```json ... ``` ````
- Prefix/suffix junk: `"json 입니다~ {...} 감사합니다"`
- “Almost JSON”: single quotes, unquoted keys, trailing commas
- Python literals: `True/False/None`
- Missing commas
- Smart quotes: `“ ”`
- Missing closing brackets/quotes

The benchmark uses a fixed set of 10 cases (see `LLM_MESSY_CASES` in `benchmarks/bench.py`).

### Metrics

- `success`: no exception was raised
- `correct`: parsed value equals the expected Python object
- `best time / case`: the best observed per-case attempt time (lower is better)

### Concrete example (drop-in impact)

**Input (not strict JSON):**

````text
preface```json
{"a":1}
```suffix
````

Strict parsers:

```python
import json
import orjson  # real orjson

json.loads('preface```json\n{"a":1}\n```suffix')     # -> JSONDecodeError
orjson.loads('preface```json\n{"a":1}\n```suffix')   # -> JSONDecodeError
```

With `agentjson` as an `orjson` drop-in (same call site):

```python
import os
import orjson  # provided by agentjson (drop-in)

os.environ["JSONPROB_ORJSON_MODE"] = "auto"
orjson.loads('preface```json\n{"a":1}\n```suffix')   # -> {"a": 1}
```

This is the core PR pitch: **don’t change code**, just switch the package and flip a mode when needed.

### Example results (2025-12-14, Python 3.12.0, macOS 14.1 arm64)

| Library / mode | Success | Correct | Best time / case |
|---|---:|---:|---:|
| `json` (strict) | 0/10 | 0/10 | n/a |
| `ujson` (strict) | 0/10 | 0/10 | n/a |
| `orjson` (strict, real) | 0/10 | 0/10 | n/a |
| `agentjson` (drop-in `orjson.loads`, mode=auto) | 10/10 | 10/10 | 23.5 µs |
| `agentjson.parse(mode=auto)` | 10/10 | 10/10 | 19.5 µs |
| `agentjson.parse(mode=probabilistic)` | 10/10 | 10/10 | 19.5 µs |

## Suite 2 — Top‑K repair suite (secondary)

### What it tests

When input is ambiguous, a “repair” might have multiple plausible interpretations.

`agentjson` can return multiple candidates (`top_k`) with confidence scores, so downstream code can:

- validate with schema/business rules,
- pick the best candidate,
- or decide to re-ask an LLM only when confidence is low.

### Concrete example (why Top‑K matters)

**Input (ambiguous suffix):**

```text
{"a":1,"b":2,"c":3, nonsense nonsense
```

A plausible “best effort” interpretation is to **truncate** the garbage suffix and return:

```python
{"a": 1, "b": 2, "c": 3}
```

But another plausible interpretation is to treat `nonsense` as a missing key/value pair and produce something else.

In the benchmark run, this case shows up exactly as:

- **Top‑1 hit** misses (not the expected value),
- but **Top‑K hit (K=5)** succeeds (the expected value is present in the candidate list).

### Example results (2025-12-14, Python 3.12.0, macOS 14.1 arm64)

| Metric | Value |
|---|---:|
| Top‑1 hit rate | 7/8 |
| Top‑K hit rate (K=5) | 8/8 |
| Avg candidates returned | 1.25 |
| Avg best confidence | 0.57 |
| Best time / case | 38.2 µs |

## Suite 3 — Large root-array parsing (big data angle)

### What it tests

This suite generates a **single large root array** like:

```json
[{"id":0,"value":"test"},{"id":0,"value":"test"}, ...]
```

and measures how long `loads(...)` takes for sizes like 5MB and 20MB.

For comparing `json/ujson/orjson`, use **Env A (real orjson)**. In Env B, `import orjson` is the shim.

### Example results (Env A: real `orjson`, 2025-12-14)

| Library | 5 MB | 20 MB |
|---|---:|---:|
| `json.loads(str)` | 53.8 ms | 217.2 ms |
| `ujson.loads(str)` | 45.9 ms | 173.7 ms |
| `orjson.loads(bytes)` (real) | 27.0 ms | 116.2 ms |

`benchmarks/bench.py` also measures `agentjson.scale(serial|parallel)` (Env B). On 5–20MB inputs the crossover depends on your machine; it’s intended for much larger payloads (GB‑scale root arrays).

## Suite 3b — Nested `corpus` suite (targeted huge value)

This is the “realistic Slack payload” shape:

```json
{ "corpus": [ ... huge ... ], "x": 0 }
```

Why this matters:

- Parallelizing a *root array* is useful, but many real payloads wrap the big array under a key (`corpus`, `rows`, `events`, …).
- The `scale_target_keys=["corpus"]` option exists to target that nested value.

`benchmarks/bench.py` includes `nested_corpus_suite` which compares:

- `agentjson.scale_pipeline(no_target)` — baseline (no targeting)
- `agentjson.scale_pipeline(corpus, serial)` — targeting without forcing parallel
- `agentjson.scale_pipeline(corpus, parallel)` — optional: targeting with forced parallel (`allow_parallel=True`, set `BENCH_NESTED_FORCE_PARALLEL=1`)

Important nuance:

- This suite uses **DOM** mode (`scale_output="dom"`) so `split_mode` shows whether nested targeting triggered (see `rust/src/scale.rs::try_nested_target_split`).
- Wiring nested targeting into **tape** mode (`scale_output="tape"`) is the next-step work for true “huge nested value without DOM” workloads.
