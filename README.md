# agentjson
![logo](./assets/logo.jpg)

## JSON parser in Rust with automatic fix for agent‑generated outputs

LLMs are great at *structured-ish* output, but real pipelines still see markdown fences, extra prose (“Here’s the JSON…”), trailing commas/smart quotes, missing commas/closers, etc. Strict parsers (`json`, `orjson`, …) treat that as a hard failure → retries, latency, and brittle tool/function-calls.

`agentjson` is a Rust-powered JSON repair pipeline with Python bindings:

- Extract the JSON span from arbitrary text
- Repair common errors cheaply first (deterministic heuristics)
- Recover intent via probabilistic **Top‑K** parsing + confidence + repair trace
- Optionally ask an LLM for a minimal byte-offset patch only when needed, then re-validate

## Features

- **Extraction**: Strip markdown fences + prefix/suffix garbage and isolate the JSON span
- **Fast path**: Valid JSON parses immediately
- **Heuristic repair**: Low-cost automatic fixes applied before beam search
- **Probabilistic Top‑K repair**: Returns multiple candidates with confidence scores + repair traces
- **Schema-aware ranking (optional)**: Lightweight schema hints help choose the right candidate
- **Deterministic mode (seeded)**: Make probabilistic results reproducible via `deterministic_seed`
- **LLM fallback (optional)**: Ask an LLM for a minimal patch only when local repairs are low-confidence
- **Scale pipeline (huge JSON)**: Safe split-point parallelism + optional tape/IR, with recursive parsing for large nested containers

### Built for LLM Pipelines

- Accepts raw model text (not just pure JSON) and extracts the JSON span
- Produces **strict JSON** (or returns Top‑K strict candidates), so downstream schema validation stays simple
- Returns a repair trace (ops + byte spans) that’s useful for debugging, audits, or “show the model what you meant”
- Uses an LLM only as a *last resort* (minimal patch + re-validate), keeping latency/cost predictable

In the included “LLM messy JSON” suite, strict parsers fail while `agentjson` succeeds end‑to‑end (see **Benchmarks** below).

### Common LLM Failure Modes

| Issue | Example | Fixed |
|-------|---------|-------|
| Unquoted keys | `{name: "Alice"}` | `{"name": "Alice"}` |
| Single quotes | `{'key': 'value'}` | `{"key": "value"}` |
| Python literals | `{"a": True, "b": None}` | `{"a": true, "b": null}` |
| Trailing commas | `{"a": 1, "b": 2,}` | `{"a": 1, "b": 2}` |
| Missing commas | `{"a": 1 "b": 2}` | `{"a": 1, "b": 2}` |
| JS comments | `{/* comment */ "a": 1}` | `{"a": 1}` |
| Unquoted array values | `[admin, user]` | `["admin", "user"]` |
| Markdown code fences | `` ```json {...} ``` `` | `{...}` |
| Prefix/suffix garbage | `Response: {...} EOF` | `{...}` |
| Unclosed strings/brackets | `{"a": "hello` | `{"a": "hello"}` |

## Installation

### Install (recommended)

```bash
uv add agentjson
# or: python -m pip install agentjson
```

### Build from source (development)

#### 1) Install Rust toolchain

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

#### 2) Build and install the PyO3 extension

```bash
# Clone the repository
uv venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows

# Install maturin and build
uv pip install maturin
maturin develop

# Install the Python package (editable)
uv pip install -e .
```

## Quick Start

![explanation](./assets/explanation.jpg)

### Python Library

```python
from agentjson import RepairOptions, parse

# Simple usage
result = parse('{"a": 1, "b": 2,}')  # trailing comma
print(result.status)           # "repaired"
print(result.best.value)       # {'a': 1, 'b': 2}

# With options
result = parse(
    '''```json
    {
        name: "Alice",
        age: 30,
        active: True,
        roles: [admin, user,]
    }
    ```''',
    RepairOptions(
        mode="auto",
        top_k=3,
        beam_width=32,
        max_repairs=50,
    ),
)

print(result.status)                    # "repaired"
print(result.best.value)                # {'name': 'Alice', 'age': 30, ...}
print(len(result.best.repairs))         # number of repairs applied
print(result.metrics.elapsed_ms)        # processing time
```

### Reproducible Top‑K (deterministic_seed)

Beam search can have ties; for debugging and stable output ordering, set `deterministic_seed`:

```python
result = parse(
    '{"a": 1 "b": 2}',  # missing comma
    RepairOptions(
        mode="probabilistic",
        top_k=5,
        deterministic_seed=42,
    ),
)
```

### Schema Hints (pick the right candidate)

When input is ambiguous, return Top‑K and let `agentjson` re-rank candidates using a lightweight schema hint:

```python
schema = {
    "required_keys": ["name", "age"],
    "types": {"name": "str", "age": "int"},
}

result = parse(
    '```json\n{name: "Alice", age: 30,}\n```',
    RepairOptions(mode="probabilistic", top_k=5, schema=schema),
)

print(result.best.validations.schema_match)  # 0.0 .. 1.0
```

### CLI

```bash
# From stdin
echo '{"a": 1, "b": 2,}' | agentjson

# From file
agentjson --input broken.json

# With options
agentjson --input broken.json \
    --mode probabilistic \
    --beam-width 64 \
    --max-repairs 100 \
    --top-k 5
```

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--input`, `-i` | stdin | Input file path |
| `--mode` | `auto` | `auto`, `strict_only`, `fast_repair`, `probabilistic`, `scale_pipeline` |
| `--scale-output` | `dom` | `dom` (materialize JSON) or `tape` (return IR only; value will be null) |
| `--top-k` | 5 | Number of candidate repairs to return |
| `--beam-width` | 32 | Beam search width |
| `--max-repairs` | 20 | Maximum repair operations per candidate |
| `--partial-ok` | true | Allow partial results on failure |
| `--allow-llm` | false | Enable LLM fallback for extreme cases |
| `--llm-provider` | `none` | `none`, `anthropic`, `claude_agent_sdk` |
| `--llm-mode` | `patch_suggest` | `patch_suggest` or `token_suggest` (patch is recommended) |
| `--llm-min-confidence` | 0.2 | Trigger LLM when best confidence is below this |
| `--debug` | false | Include debug information |

### What is `tape`?

`tape` is an internal **IR (intermediate representation)** for large JSON:

- A flat list of `TapeEntry`s (token type + byte `offset`/`length` into the original input).
- Containers (`array_start` / `object_start`) store a “jump” payload to their matching end entry.
- This makes it cheaper to handle huge payloads (avoid building a full in-memory DOM) and enables safe parallel parse+merge in `scale_pipeline`.

When `scale_output="tape"`:

- `result.best.value` is `None`
- `result.best.ir["tape"]` contains tape metadata (and, with `debug=True`, a truncated preview of entries)

### FAQ (LLM + JSON)

**“We already use structured output / function calling. Why do we need this?”**  
Because in production you still get *near-JSON* (code fences, extra prose, a trailing comma, a missing closer). Strict JSON parsing turns that into retries (latency/cost) or brittle failures. `agentjson` is the guardrail: it converts raw model text into strict JSON (or Top‑K strict candidates) and tells you exactly what it changed.

**“Why Top‑K?”**  
When JSON is corrupted, there can be multiple plausible “intents”. Returning Top‑K candidates + confidence (and optional schema hints) lets you pick the right one deterministically instead of guessing.

**“Is the scale pipeline always faster?”**  
No—parallel split/merge has overhead. It’s designed for huge valid JSON (GB‑scale root arrays or large nested containers) where scan/parse time dominates. For small inputs, strict parsing is faster.

### Rust CLI (development) — mmap + deterministic seed

For batch parsing of very large **files** without allocating a giant `Vec<u8>` up front, the Rust CLI in `rust/` uses **mmap by default**:

```bash
cd rust
cargo build --release
./target/release/agentjson --input huge.json --mode scale_pipeline --scale-output tape
```

- Disable mmap: `--no-mmap`
- Reproducible beam ordering: `--deterministic-seed 42`

## orjson Drop-in

Most LLM/agent stacks already call `orjson.loads()` everywhere. `agentjson` provides an `orjson`-compatible drop-in module so you can keep those call sites unchanged and still recover from “near‑JSON” outputs:

```python
import orjson

data = orjson.loads(b'{"a": 1}')
blob = orjson.dumps({"a": 1})
```

If you prefer to be explicit (or want to avoid `orjson` name conflicts), you can also do:

```python
import agentjson as orjson
```

By default the shim is strict (like real `orjson`). To enable repair/scale fallback without changing call sites:

```bash
export JSONPROB_ORJSON_MODE=auto
```

See `demo/orjson_dropin_demo.py` for a concrete example.

## Benchmarks

Benchmarks were run on **Python 3.12.0**, **macOS 14.1 (arm64)** using `benchmarks/bench.py`.

For a detailed walkthrough with concrete Slack-context examples, see `BENCHMARK.md`.

### 1) LLM messy JSON suite (primary)

This suite reflects the context: LLM outputs like “json입니다~ …”, markdown fences, single quotes, unquoted keys, trailing commas, Python literals, missing commas, smart quotes, and missing closers.

| Library / mode | Success | Correct | Best time / case |
|---|---:|---:|---:|
| `json` (strict) | 0/10 | 0/10 | n/a |
| `ujson` (strict) | 0/10 | 0/10 | n/a |
| `orjson` (strict, real) | 0/10 | 0/10 | n/a |
| `agentjson` (drop-in `orjson.loads`, mode=auto) | 10/10 | 10/10 | 23.5 µs |
| `agentjson.parse(mode=auto)` | 10/10 | 10/10 | 19.5 µs |
| `agentjson.parse(mode=probabilistic)` | 10/10 | 10/10 | 19.5 µs |

Key point: **drop-in call sites** (`import orjson; orjson.loads(...)`) can go from *0% success* → *100% success* just by setting `JSONPROB_ORJSON_MODE=auto`.

### 2) Top‑K repair suite (secondary)

This suite checks whether the “intended” JSON object is recovered as the **best candidate** vs anywhere in the **Top‑K (K=5)** candidates.

| Metric | Value |
|---|---:|
| Top‑1 hit rate | 7/8 |
| Top‑K hit rate (K=5) | 8/8 |
| Avg candidates returned | 1.25 |
| Avg best confidence | 0.57 |
| Best time / case | 38.2 µs |

### 3) Large root-array parsing (big data angle)

Valid JSON only (parsing a single large root array).

| Library | 5 MB | 20 MB |
|---|---:|---:|
| `json.loads(str)` | 53.8 ms | 217.2 ms |
| `ujson.loads(str)` | 45.9 ms | 173.7 ms |
| `orjson.loads(bytes)` (real) | 27.0 ms | 116.2 ms |

`agentjson` also benchmarks `agentjson.scale(serial|parallel)` in the same script. On 5–20MB inputs the crossover depends on your machine: on this run the parallel path is slower at 5MB and slightly faster at 20MB; it’s intended for much larger payloads (GB‑scale root arrays).

### 3b) Nested `corpus` split (targeted huge value)

If your payload looks like `{ "corpus": [ ... huge ... ], ... }`, `benchmarks/bench.py` includes a `nested_corpus_suite` that benchmarks `scale_target_keys=["corpus"]`. This is the practical “nested huge value” case from the Slack thread.

Today, nested targeting is benchmarked in `scale_output="dom"` (it records `split_mode` like `NESTED_KEY(corpus).…`). Wiring nested targeting into `scale_output="tape"` is the next step for true “huge nested value without DOM” workloads.

### 3c) CLI mmap suite (PR‑006)

If you care about **batch/CLI parsing of very large files** without allocating a giant `Vec<u8>` up front, set `BENCH_CLI_MMAP_MB` to run `cli_mmap_suite` (default mmap vs `--no-mmap`). You need the Rust CLI binary built first:

```bash
cd rust && cargo build --release
```

#### Reproduce

Because `agentjson` provides a top-level `orjson` shim, benchmark real `orjson` and the shim in separate environments:

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

## Repair Pipeline

```
Input Text
    │
    ▼
┌─────────────────┐
│ 1. Extraction   │  Strip markdown fences, prefix/suffix garbage
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 2. Heuristics   │  Fast fixes: quotes, comments, literals, commas
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 3. Strict Parse │  Try standard JSON parse
└────────┬────────┘
         │ (if fails)
         ▼
┌─────────────────┐
│ 4. Beam Search  │  Probabilistic repair with Top-K candidates
└────────┬────────┘
         │ (if low confidence)
         ▼
┌─────────────────┐
│ 5. LLM Fallback │  Optional: Claude-assisted repair
└────────┬────────┘
         │
         ▼
    RepairResult
```

## LLM Deep Repair (Optional)

For severely corrupted JSON where beam search is low-confidence, you can enable LLM-assisted repair.

### Option A) Anthropic SDK

```bash
python -m pip install anthropic
export ANTHROPIC_API_KEY=...
export CLAUDE_MODEL=claude-3-5-sonnet-latest
```

```python
from agentjson import AnthropicPatchSuggestProvider, RepairOptions, parse

result = parse(
    '{"a":1,"b":2, completely broken garbage here',
    RepairOptions(
        mode="probabilistic",
        allow_llm=True,
        llm_mode="patch_suggest",
        llm_min_confidence=0.2,
        llm_provider=AnthropicPatchSuggestProvider(),
    ),
)

print(result.metrics.llm_calls)
print(result.metrics.llm_time_ms)
```

### Option B) Claude Agent SDK

```python
from agentjson import RepairOptions, parse
from agentjson.claude_agent_sdk_provider import ClaudeAgentSDKProvider

# Set up your Claude Agent SDK agent
agent = ...  # your agent instance
provider = ClaudeAgentSDKProvider(agent=agent)

result = parse(
    '{"a":1,"b":2, completely broken garbage here',
    RepairOptions(
        mode="probabilistic",
        allow_llm=True,
        llm_mode="patch_suggest",
        llm_min_confidence=0.2,
        llm_provider=provider,
    ),
)

print(result.metrics.llm_calls)     # number of LLM calls made
print(result.metrics.llm_time_ms)   # LLM processing time
```

## Result Structure

```python
result = parse(text, options)

result.status          # "strict_ok" | "repaired" | "partial" | "failed"
result.best            # Best candidate (shortcut for candidates[best_index])
result.best_index      # Index of best candidate
result.candidates      # List of repair candidates

# Each candidate has:
candidate.value           # Parsed Python object
candidate.normalized_json # Normalized JSON string
candidate.confidence      # Confidence score (0-1)
candidate.cost           # Total repair cost
candidate.repairs        # List of repair operations applied

# Each repair operation:
repair.op        # Operation name (e.g., "wrap_unquoted_key")
repair.span      # (start, end) byte positions
repair.cost_delta # Cost of this repair
repair.note      # Human-readable description
```

## Development

### Run Tests

```bash
# Rust tests
cd rust && cargo test

# Python tests (parse tests are skipped unless PyO3 is installed)
PYTHONPATH=src python -m unittest discover -s tests -p 'test*.py' -v
```

### Build Rust CLI (standalone)

```bash
cd rust
cargo build --release
./target/release/agentjson --input ../demo/broken.json
```

## Architecture

```
agentjson/
├── rust/                    # Core Rust library
│   └── src/
│       ├── heuristic.rs     # Heuristic repairs
│       ├── beam.rs          # Beam search algorithm
│       ├── pipeline.rs      # Parse pipeline orchestration
│       └── ...
├── rust-pyo3/               # PyO3 Python bindings
│   └── src/lib.rs
└── src/json_prob_parser/    # Python package
    ├── pipeline.py          # Python pipeline (Rust + optional LLM)
    ├── rust_core.py         # Thin PyO3 bridge
    ├── anthropic_provider.py
    ├── claude_agent_sdk_provider.py
    ├── llm.py               # LLM payload + patch ops
    └── types.py             # Data classes
```

## License

MIT OR Apache-2.0
