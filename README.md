# json-prob-parser

JSON parsing and repair:

- Fast path: strict JSON parses return immediately
- Repair path: heuristic fixes (trailing comma, EOF closers, comments, python literals)
- Probabilistic path: beam-search repair that returns Top‑K candidates with:
  - `cost` and `confidence`
  - `repairs[]` trace (ops + spans/offsets)
  - optional `schema_match` scoring
  - partial mode via suffix truncation + container closure

## Install (editable)

```bash
python -m pip install -e .
```

## CLI

```bash
cat input.txt | json-prob-parser --mode auto --top-k 5
json-prob-parser --input broken.json --mode probabilistic --beam-width 48
```

## Library

```python
from json_prob_parser import parse, RepairOptions

result = parse(
    '```json\n{a: "x",}\n``` trailing',
    RepairOptions(mode="auto", top_k=3, beam_width=32, max_repairs=20, partial_ok=True),
)

print(result.status)
print(result.best.value)
print(result.best.repairs)
```

## LLM Deep Repair (Phase 3, optional)

The parser can optionally call an LLM **only when probabilistic repair fails or is low-confidence**.

You can plug in the Claude Agent SDK (tools: memory, web search, skills, etc.) by passing an agent-backed
provider via `RepairOptions.llm_provider`:

```python
from json_prob_parser import parse, RepairOptions
from json_prob_parser.claude_agent_sdk_provider import ClaudeAgentSDKProvider

# Construct your Claude Agent SDK agent in your app (with tools/memory/search configured),
# then wrap it. The agent only needs to implement agent.run(prompt) (or .invoke / __call__).
agent = ...  # your Agent SDK agent instance
provider = ClaudeAgentSDKProvider(agent=agent)

opt = RepairOptions(
    mode="probabilistic",
    allow_llm=True,
    llm_mode="patch_suggest",
    llm_min_confidence=0.2,
    llm_provider=provider,
)

result = parse('{"a":1,"b":2, nonsense', opt)
print(result.status, result.metrics.llm_calls, result.metrics.llm_time_ms)
```

## Run tests

Without installing:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test*.py' -v
```
