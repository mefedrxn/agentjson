from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from typing import Optional

from .anthropic_provider import AnthropicPatchSuggestProvider
from .pipeline import parse
from .types import RepairOptions


def _read_input(path: Optional[str]) -> str:
    if not path or path == "-":
        return sys.stdin.read()
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def _read_input_bytes(path: Optional[str]) -> bytes:
    if not path or path == "-":
        return sys.stdin.buffer.read()
    with open(path, "rb") as f:
        return f.read()


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default="-", help="Input file (default: stdin)")
    p.add_argument("--mode", default="auto", help="auto|strict_only|fast_repair|probabilistic|scale_pipeline")
    p.add_argument("--scale-output", default="dom", choices=["dom", "tape"], help="scale_pipeline output: dom|tape")
    p.add_argument("--top-k", type=int, default=5)
    p.add_argument("--beam-width", type=int, default=32)
    p.add_argument("--max-repairs", type=int, default=20)
    p.add_argument("--max-deleted-tokens", type=int, default=3)
    p.add_argument("--max-close-open-string", type=int, default=1)
    p.add_argument("--max-garbage-skip-bytes", type=int, default=8 * 1024)
    p.add_argument("--confidence-alpha", type=float, default=0.7)
    p.add_argument("--min-elements-for-parallel", type=int, default=512)
    p.add_argument("--density-threshold", type=float, default=0.001)
    p.add_argument("--parallel-chunk-bytes", type=int, default=8 * 1024 * 1024)
    p.add_argument("--parallel-workers", type=int, default=0)
    p.add_argument("--parallel-backend", default="process", choices=["process", "thread"])
    p.add_argument("--partial-ok", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--allow-llm", action=argparse.BooleanOptionalAction, default=False)
    p.add_argument("--llm-mode", default="patch_suggest", help="patch_suggest|token_suggest")
    p.add_argument("--llm-min-confidence", type=float, default=0.2)
    p.add_argument("--llm-provider", default="none", choices=["none", "anthropic", "claude_agent_sdk"])
    p.add_argument("--debug", action=argparse.BooleanOptionalAction, default=False)
    args = p.parse_args(argv)

    text = _read_input_bytes(args.input) if args.mode == "scale_pipeline" else _read_input(args.input)
    opt = RepairOptions(
        mode=args.mode,
        scale_output=args.scale_output,
        top_k=args.top_k,
        beam_width=args.beam_width,
        max_repairs=args.max_repairs,
        max_deleted_tokens=args.max_deleted_tokens,
        max_close_open_string=args.max_close_open_string,
        max_garbage_skip_bytes=args.max_garbage_skip_bytes,
        min_elements_for_parallel=args.min_elements_for_parallel,
        density_threshold=args.density_threshold,
        parallel_chunk_bytes=args.parallel_chunk_bytes,
        parallel_workers=(args.parallel_workers or None),
        parallel_backend=args.parallel_backend,
        confidence_alpha=args.confidence_alpha,
        partial_ok=args.partial_ok,
        allow_llm=args.allow_llm,
        llm_mode=args.llm_mode,
        llm_min_confidence=args.llm_min_confidence,
        debug=args.debug,
    )

    if args.allow_llm:
        if args.llm_provider == "anthropic":
            opt.llm_provider = AnthropicPatchSuggestProvider()
        elif args.llm_provider == "claude_agent_sdk":
            from .claude_agent_sdk_provider import ClaudeAgentSDKProvider

            opt.llm_provider = ClaudeAgentSDKProvider.from_env()

    try:
        result = parse(text, opt)
    except RuntimeError as e:
        print(str(e).rstrip(), file=sys.stderr)
        return 2

    print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    return 0 if result.status != "failed" else 2
