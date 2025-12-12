import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from json_prob_parser import RepairOptions, parse  # noqa: E402
from json_prob_parser import apply_patch_ops_utf8  # noqa: E402


class TestArbiter(unittest.TestCase):
    def test_strict_ok(self):
        r = parse('{"a":1}')
        self.assertEqual(r.status, "strict_ok")
        self.assertEqual(r.best.value["a"], 1)

    def test_code_fence_extract(self):
        r = parse("preface```json\n{\"a\":1}\n```suffix", RepairOptions(debug=True))
        self.assertIn(r.status, ("repaired", "strict_ok"))
        self.assertEqual(r.best.value["a"], 1)

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


if __name__ == "__main__":
    unittest.main()
