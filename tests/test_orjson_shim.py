import json
import os
import sys
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import orjson  # noqa: E402
import agentjson  # noqa: E402


class TestOrjsonShim(unittest.TestCase):
    def test_agentjson_import_alias(self):
        # Distribution name is `agentjson`, but core module remains `json_prob_parser`.
        self.assertTrue(hasattr(agentjson, "parse"))

    def test_loads_accepts_input_types(self):
        expected = {"a": 1}
        inputs = [
            b'{"a":1}',
            bytearray(b'{"a":1}'),
            memoryview(b'{"a":1}'),
            '{"a":1}',
        ]
        for obj in inputs:
            self.assertEqual(orjson.loads(obj), expected)

    def test_loads_invalid_type_raises_jsondecodeerror(self):
        with self.assertRaises(orjson.JSONDecodeError) as ctx:
            orjson.loads(123)
        self.assertIsInstance(ctx.exception, json.JSONDecodeError)
        self.assertIsInstance(ctx.exception, ValueError)
        self.assertEqual(ctx.exception.pos, 0)

    def test_loads_invalid_utf8_bytes(self):
        with self.assertRaises(orjson.JSONDecodeError) as ctx:
            orjson.loads(b"\xff")
        self.assertIsInstance(ctx.exception, json.JSONDecodeError)
        self.assertEqual(ctx.exception.doc, "")
        self.assertEqual(ctx.exception.pos, 0)

    def test_loads_invalid_json(self):
        with self.assertRaises(orjson.JSONDecodeError) as ctx:
            orjson.loads(b'{"a":}')
        self.assertEqual(ctx.exception.doc, '{"a":}')
        self.assertIsInstance(ctx.exception.pos, int)

    def test_dumps_returns_bytes(self):
        self.assertIsInstance(orjson.dumps({"a": 1}), (bytes,))

    def test_dumps_append_newline(self):
        out = orjson.dumps({"a": 1}, option=orjson.OPT_APPEND_NEWLINE)
        self.assertTrue(out.endswith(b"\n"))

    def test_dumps_indent_2(self):
        out = orjson.dumps({"a": 1}, option=orjson.OPT_INDENT_2)
        self.assertEqual(out, b'{\n  "a": 1\n}')

    def test_dumps_sort_keys(self):
        out = orjson.dumps({"b": 1, "a": 2}, option=orjson.OPT_SORT_KEYS)
        self.assertEqual(out, b'{"a":2,"b":1}')

    def test_dumps_strict_integer(self):
        limit_plus_one = (1 << 53)
        with self.assertRaises(orjson.JSONEncodeError) as ctx:
            orjson.dumps({"n": limit_plus_one}, option=orjson.OPT_STRICT_INTEGER)
        self.assertIsInstance(ctx.exception, TypeError)

    def test_dumps_datetime_options(self):
        naive = datetime(2020, 1, 2, 3, 4, 5, 123456)
        aware = datetime(2020, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc)

        self.assertEqual(orjson.dumps({"t": naive}), b'{"t":"2020-01-02T03:04:05.123456"}')
        self.assertEqual(
            orjson.dumps({"t": naive}, option=orjson.OPT_NAIVE_UTC),
            b'{"t":"2020-01-02T03:04:05.123456+00:00"}',
        )
        self.assertEqual(
            orjson.dumps({"t": aware}, option=orjson.OPT_UTC_Z),
            b'{"t":"2020-01-02T03:04:05.123456Z"}',
        )
        self.assertEqual(
            orjson.dumps({"t": aware}, option=orjson.OPT_UTC_Z | orjson.OPT_OMIT_MICROSECONDS),
            b'{"t":"2020-01-02T03:04:05Z"}',
        )

    def test_dumps_dataclass(self):
        @dataclass
        class A:
            x: int

        self.assertEqual(orjson.dumps({"a": A(1)}), b'{"a":{"x":1}}')

    def test_dumps_non_str_keys(self):
        with self.assertRaises(orjson.JSONEncodeError):
            orjson.dumps({("a", "b"): 1})
        out = orjson.dumps({("a", "b"): 1}, option=orjson.OPT_NON_STR_KEYS)
        self.assertEqual(out.decode("utf-8"), "{\"('a', 'b')\":1}")

    def test_fragment_insertion(self):
        out = orjson.dumps({"a": orjson.Fragment(b'{"x":1}')})
        self.assertEqual(out, b'{"a":{"x":1}}')


if __name__ == "__main__":
    unittest.main()
