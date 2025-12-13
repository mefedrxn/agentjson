from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Literal, Optional, Tuple, Union

JsonValue = Any

Mode = Literal["auto", "strict_only", "fast_repair", "probabilistic", "scale_pipeline"]
Status = Literal["strict_ok", "repaired", "partial", "failed"]


@dataclass
class RepairAction:
    op: str
    span: Optional[Tuple[int, int]] = None
    at: Optional[int] = None
    token: Optional[str] = None
    cost_delta: float = 0.0
    note: Optional[str] = None


@dataclass
class CandidateValidations:
    strict_json_parse: bool
    schema_match: Optional[float] = None


@dataclass
class CandidateDiagnostics:
    garbage_skipped_bytes: int = 0
    deleted_tokens: int = 0
    inserted_tokens: int = 0
    close_open_string_count: int = 0
    beam_width: Optional[int] = None
    max_repairs: Optional[int] = None


@dataclass
class Candidate:
    candidate_id: int
    value: Optional[JsonValue] = None
    normalized_json: Optional[str] = None
    ir: Optional[Any] = None
    confidence: float = 0.0
    cost: float = 0.0
    repairs: list[RepairAction] = field(default_factory=list)
    validations: CandidateValidations = field(default_factory=lambda: CandidateValidations(strict_json_parse=False))
    diagnostics: CandidateDiagnostics = field(default_factory=CandidateDiagnostics)
    dropped_spans: list[Tuple[int, int]] = field(default_factory=list)


@dataclass
class InputStats:
    input_bytes: int
    extracted_span: Tuple[int, int]
    prefix_skipped_bytes: int = 0
    suffix_skipped_bytes: int = 0


@dataclass
class PartialResult:
    extracted: Optional[JsonValue] = None
    dropped_spans: list[Tuple[int, int]] = field(default_factory=list)


@dataclass
class ParseError:
    kind: str
    at: Optional[int] = None
    message: Optional[str] = None


@dataclass
class Metrics:
    mode_used: str
    elapsed_ms: int
    beam_width: int = 0
    max_repairs: int = 0
    llm_calls: int = 0
    llm_time_ms: int = 0
    llm_trigger: Optional[str] = None
    split_mode: str = ""
    parallel_workers: int = 0
    elements: int = 0
    structural_density: float = 0.0


@dataclass
class RepairOptions:
    mode: Mode = "auto"
    top_k: int = 5
    beam_width: int = 32
    max_repairs: int = 20
    max_deleted_tokens: int = 3
    max_close_open_string: int = 1
    max_garbage_skip_bytes: int = 8 * 1024
    min_elements_for_parallel: int = 512
    density_threshold: float = 0.001
    parallel_chunk_bytes: int = 8 * 1024 * 1024
    parallel_workers: Optional[int] = None
    parallel_backend: Literal["process", "thread"] = "process"
    scale_output: str = "dom"  # dom|tape (scale_pipeline only)
    scale_target_keys: Optional[list[str]] = None
    partial_ok: bool = True
    allow_single_quotes: bool = True
    allow_unquoted_keys: bool = True
    allow_unquoted_values: bool = True
    allow_comments: bool = True
    allow_python_literals: bool = True
    allow_parallel: Union[Literal["auto"], bool] = "auto"
    parallel_threshold_bytes: int = 1_000_000_000
    allow_llm: bool = False
    max_llm_calls_per_doc: int = 2
    llm_timeout_ms: int = 5000
    llm_mode: str = "patch_suggest"  # token_suggest|patch_suggest
    llm_min_confidence: float = 0.2
    llm_provider: Optional[Callable[[dict], Any]] = None
    confidence_alpha: float = 0.7
    schema: Optional[dict] = None
    deterministic_seed: int = 0
    debug: bool = False


@dataclass
class RepairResult:
    status: Status
    best_index: Optional[int]
    input_stats: InputStats
    candidates: list[Candidate] = field(default_factory=list)
    partial: Optional[PartialResult] = None
    errors: list[ParseError] = field(default_factory=list)
    metrics: Metrics = field(default_factory=lambda: Metrics(mode_used="strict", elapsed_ms=0))
    debug: Optional[dict] = None

    @property
    def best(self) -> Optional[Candidate]:
        if self.best_index is None:
            return None
        if 0 <= self.best_index < len(self.candidates):
            return self.candidates[self.best_index]
        return None
