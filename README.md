# json-prob-parser

A probabilistic JSON repair library powered by Rust with Python bindings. Handles severely broken JSON that standard parsers reject.

## Features

- **Fast path**: Valid JSON parses immediately
- **Heuristic repair**: Automatic fixes applied before beam search
- **Probabilistic beam search**: Returns Top-K repair candidates with confidence scores
- **LLM fallback**: Optional LLM-assisted repair for extreme cases

### What It Can Fix

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

### 1. Install Rust toolchain

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### 2. Install the package with PyO3 extension

```bash
# Clone the repository
git clone https://github.com/anthropics/json-prob-parser.git
cd json-prob-parser

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows

# Install maturin and build
pip install maturin
maturin develop -m rust-pyo3/Cargo.toml

# Install the Python package
pip install -e .
```

## Quick Start

### Python Library

```python
from json_prob_parser import parse, RepairOptions

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

### CLI

```bash
# From stdin
echo '{"a": 1, "b": 2,}' | json-prob-parser

# From file
json-prob-parser --input broken.json

# With options
json-prob-parser --input broken.json \
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
| `--top-k` | 5 | Number of candidate repairs to return |
| `--beam-width` | 32 | Beam search width |
| `--max-repairs` | 20 | Maximum repair operations per candidate |
| `--partial-ok` | true | Allow partial results on failure |
| `--allow-llm` | false | Enable LLM fallback for extreme cases |
| `--debug` | false | Include debug information |

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

For severely corrupted JSON where beam search fails, you can enable LLM-assisted repair:

```python
from json_prob_parser import parse, RepairOptions
from json_prob_parser.claude_agent_sdk_provider import ClaudeAgentSDKProvider

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

# Python tests (after building PyO3)
PYTHONPATH=src python -m pytest tests/ -v
```

### Build Rust CLI (standalone)

```bash
cd rust
cargo build --release
./target/release/json-prob-parser --input ../demo/broken.json
```

## Architecture

```
json-prob-parser/
├── rust/                    # Core Rust library
│   └── src/
│       ├── heuristic.rs     # Heuristic repairs
│       ├── beam.rs          # Beam search algorithm
│       ├── arbiter.rs       # Main orchestration
│       └── ...
├── rust-pyo3/               # PyO3 Python bindings
│   └── src/lib.rs
└── src/json_prob_parser/    # Python package
    ├── rust_core.py         # Rust backend wrapper
    ├── llm_arbiter.py       # LLM orchestration
    └── types.py             # Data classes
```

## License

MIT OR Apache-2.0
