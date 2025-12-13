import os
import sys
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from json_prob_parser import RepairOptions, apply_patch_ops_utf8, parse  # noqa: E402
from json_prob_parser import rust_core  # noqa: E402


@unittest.skipUnless(rust_core.HAVE_RUST, "Rust PyO3 extension (agentjson_rust) not installed")
class TestPipelineParse(unittest.TestCase):
    def test_strict_ok(self):
        r = parse('{"a":1}')
        self.assertEqual(r.status, "strict_ok")
        self.assertEqual(r.best.value["a"], 1)

    def test_code_fence_extract(self):
        r = parse("preface```json\n{\"a\":1}\n```suffix", RepairOptions(debug=True))
        self.assertIn(r.status, ("repaired", "strict_ok"))
        self.assertEqual(r.best.value["a"], 1)
        self.assertIsNotNone(r.debug)

    def test_trailing_comma_heuristic(self):
        r = parse('{"a":1,}')
        self.assertIn(r.status, ("repaired", "strict_ok"))
        self.assertEqual(r.best.value["a"], 1)

    def test_missing_closer_heuristic(self):
        r = parse('{"a":1')
        self.assertIn(r.status, ("repaired", "strict_ok"))
        self.assertEqual(r.best.value["a"], 1)

    def test_probabilistic_unquoted_key_and_single_quotes(self):
        r = parse("{a: 'b'}", RepairOptions(mode="probabilistic", top_k=3))
        self.assertIn(r.status, ("repaired", "partial"))
        self.assertEqual(r.best.value["a"], "b")

    def test_partial_truncate_suffix(self):
        r = parse(
            '{"a":1,"b":2,"c":3, nonsense nonsense',
            RepairOptions(mode="probabilistic", partial_ok=True),
        )
        self.assertEqual(r.status, "partial")
        self.assertEqual(r.best.value["a"], 1)
        self.assertEqual(r.best.value["b"], 2)
        self.assertEqual(r.best.value["c"], 3)
        self.assertTrue(r.best.dropped_spans)

    def test_fix_smart_quotes(self):
        r = parse("{“a”: “b”}")
        self.assertIn(r.status, ("repaired", "strict_ok"))
        self.assertEqual(r.best.value["a"], "b")

    def test_scale_pipeline_root_array_thread(self):
        data = b"[1, 2, 3]"
        r = parse(
            data,
            RepairOptions(
                mode="scale_pipeline",
                allow_parallel=True,
                parallel_backend="thread",
                min_elements_for_parallel=1,
                parallel_threshold_bytes=0,
                parallel_workers=2,
                parallel_chunk_bytes=1,
            ),
        )
        self.assertEqual(r.status, "strict_ok")
        self.assertEqual(r.best.value, [1, 2, 3])

    def test_scale_pipeline_nested_target_key_split(self):
        data = b'{"corpus":[1,2,3,4,5,6], "x": 0}'
        r = parse(
            data,
            RepairOptions(
                mode="scale_pipeline",
                scale_target_keys=["corpus"],
                allow_parallel=True,
                parallel_backend="thread",
                min_elements_for_parallel=1,
                parallel_threshold_bytes=0,
                parallel_workers=2,
                parallel_chunk_bytes=1,
            ),
        )
        self.assertEqual(r.status, "strict_ok")
        self.assertEqual(r.best.value["corpus"], [1, 2, 3, 4, 5, 6])
        self.assertEqual(r.best.value["x"], 0)
        self.assertTrue(str(r.metrics.split_mode).startswith("NESTED_KEY(corpus)."))

    def test_llm_deep_repair_patch_suggest(self):
        # Force an LLM call by using a high llm_min_confidence threshold.
        def provider(payload):
            snippet = payload["snippet"]["text"]
            span_start, _span_end = payload["snippet"]["span_in_extracted"]
            comma = snippet.index(", nonsense")
            last_brace = snippet.rfind("}")
            return {
                "mode": "patch_suggest",
                "patches": [
                    {
                        "patch_id": "p1",
                        "ops": [
                            {"op": "delete", "span": [span_start + comma, span_start + last_brace]},
                        ],
                        "confidence": 0.9,
                        "rationale": "drop garbage suffix",
                    }
                ],
            }

        r = parse(
            '{"a":1,"b":2, nonsense nonsense',
            RepairOptions(
                mode="probabilistic",
                allow_llm=True,
                llm_mode="patch_suggest",
                llm_min_confidence=0.99,
                llm_provider=provider,
            ),
        )
        self.assertEqual(r.metrics.llm_calls, 1)
        self.assertIn(r.status, ("repaired", "partial"))
        self.assertEqual(r.best.value["a"], 1)
        self.assertEqual(r.best.value["b"], 2)

    def test_llm_timeout(self):
        def slow_provider(_payload):
            time.sleep(0.2)
            return {"mode": "patch_suggest", "patches": []}

        r = parse(
            '{"a":1,"b":2, nonsense nonsense',
            RepairOptions(
                mode="probabilistic",
                allow_llm=True,
                llm_mode="patch_suggest",
                llm_min_confidence=1.1,
                llm_timeout_ms=10,
                llm_provider=slow_provider,
            ),
        )
        self.assertEqual(r.metrics.llm_calls, 1)
        self.assertEqual(r.metrics.llm_trigger, "low_confidence")


class TestLLMUtils(unittest.TestCase):
    def test_apply_llm_patch_ops_utf8(self):
        text = 'X{"a":1}Y'
        patched = apply_patch_ops_utf8(
            text,
            [
                {"op": "delete", "span": [0, 1]},
                {"op": "delete", "span": [len(text) - 1, len(text)]},
            ],
        )
        self.assertEqual(patched, '{"a":1}')


if __name__ == "__main__":
    unittest.main()
