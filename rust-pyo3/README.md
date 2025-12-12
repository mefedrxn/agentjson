# json-prob-parser-rust (PyO3)

Optional PyO3 bindings for the Rust core (`../rust`).

## Build / install (dev)

From repo root:

```bash
python -m pip install -U maturin
maturin develop -m rust-pyo3/Cargo.toml
```

Then in Python:

```python
import json_prob_parser_rust  # noqa: F401
```

