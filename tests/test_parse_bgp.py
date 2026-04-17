import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path


def _install_pyspark_stubs() -> None:
    pyspark_mod = types.ModuleType("pyspark")
    sql_mod = types.ModuleType("pyspark.sql")
    functions_mod = types.ModuleType("pyspark.sql.functions")
    types_mod = types.ModuleType("pyspark.sql.types")

    class DataFrame:  # pragma: no cover
        pass

    def _dummy_type(*args, **kwargs):
        return {"args": args, "kwargs": kwargs}

    sql_mod.DataFrame = DataFrame
    sql_mod.functions = functions_mod
    sql_mod.types = types_mod

    functions_mod.udf = lambda func, _schema: func

    types_mod.StructType = _dummy_type
    types_mod.StructField = _dummy_type
    types_mod.StringType = _dummy_type
    types_mod.LongType = _dummy_type
    types_mod.IntegerType = _dummy_type
    types_mod.ArrayType = _dummy_type

    sys.modules.setdefault("pyspark", pyspark_mod)
    sys.modules.setdefault("pyspark.sql", sql_mod)
    sys.modules.setdefault("pyspark.sql.functions", functions_mod)
    sys.modules.setdefault("pyspark.sql.types", types_mod)


_install_pyspark_stubs()


MODULE_PATH = Path(__file__).resolve().parents[1] / "pyspark-import" / "parse_bgp.py"
SPEC = importlib.util.spec_from_file_location("parse_bgp", MODULE_PATH)
parse_bgp = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(parse_bgp)


class TestParseBgp(unittest.TestCase):
    def test_parse_bgp_update_one_as(self) -> None:
        payload = "00000023400101005002000602010000fde8400304c0a8330180040400000000c00804007b01c8100a0a"

        result = parse_bgp.parse_bgp_update_payload(payload)

        self.assertEqual(result["as_path"], [65000])
        self.assertEqual(result["next_hop"], "192.168.51.1")
        self.assertEqual(result["nlri"], ["10.10.0.0/16"])

    def test_parse_bgp_update_multiple_as(self) -> None:
        payload = "00000024400101005002000a02020000feb00000fde8400304c0a83202c00808007b01c80141028e100a14"

        result = parse_bgp.parse_bgp_update_payload(payload)

        self.assertEqual(result["as_path"], [65200, 65000])
        self.assertEqual(result["next_hop"], "192.168.50.2")
        self.assertEqual(result["nlri"], ["10.20.0.0/16"])

    def test_parse_single_event_non_update_returns_none(self) -> None:
        raw = json.dumps(
            {
                "event_type": "bgp",
                "bgp": {"message_type": "keepalive"},
            }
        )

        self.assertIsNone(parse_bgp.parse_single_event(raw))

    def test_parse_single_event_includes_as_fields(self) -> None:
        raw = json.dumps(
            {
                "event_type": "bgp",
                "bgp": {
                    "message_type": "update",
                    "payload": "00000024400101005002000a02020000feb00000fde8400304c0a83202c00808007b01c80141028e100a14",
                },
            }
        )

        result = parse_bgp.parse_single_event(raw)

        self.assertIsNotNone(result)
        self.assertIn("bgp", result)
        self.assertIn("handle", result["bgp"])
        self.assertIn("description", result["bgp"])
        self.assertIn("country-code", result["bgp"])
        # AS 65200 and 65000 are both private ASNs
        self.assertEqual(result["bgp"]["handle"], ["AS65200", "AS65000"])
        self.assertEqual(result["bgp"]["description"], ["Private AS", "Private AS"])
        self.assertEqual(result["bgp"]["country-code"], ["", ""])

    def test_asn_info_returns_correct_tuple(self) -> None:
        asn_map = {
            1: ("LVLT-1", "Level 3 Parent LLC", "US"),
            131083: ("SOME-HANDLE", "Mercari Inc.", "JP"),
        }

        self.assertEqual(parse_bgp._asn_info(65000, asn_map), ("AS65000", "Private AS", ""))
        self.assertEqual(parse_bgp._asn_info(4200000001, asn_map), ("AS4200000001", "Private AS", ""))
        self.assertEqual(parse_bgp._asn_info(1, asn_map), ("LVLT-1", "Level 3 Parent LLC", "US"))
        self.assertEqual(parse_bgp._asn_info(999999, asn_map), ("AS999999", "AS999999", ""))
        self.assertEqual(parse_bgp._asn_info(131083, asn_map), ("SOME-HANDLE", "Mercari Inc.", "JP"))


if __name__ == "__main__":
    unittest.main()
