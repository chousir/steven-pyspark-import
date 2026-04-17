import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


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
        self.assertIsNone(result["parse_error"])

    def test_parse_bgp_update_multiple_as(self) -> None:
        payload = "00000024400101005002000a02020000feb00000fde8400304c0a83202c00808007b01c80141028e100a14"

        result = parse_bgp.parse_bgp_update_payload(payload)

        self.assertEqual(result["as_path"], [65200, 65000])
        self.assertEqual(result["next_hop"], "192.168.50.2")
        self.assertEqual(result["nlri"], ["10.20.0.0/16"])
        self.assertIsNone(result["parse_error"])

    def test_parse_bgp_update_empty_payload_has_no_error(self) -> None:
        result = parse_bgp.parse_bgp_update_payload(None)
        self.assertIsNone(result["parse_error"])

    def test_parse_bgp_update_invalid_hex_sets_parse_error(self) -> None:
        result = parse_bgp.parse_bgp_update_payload("not-valid-hex!")
        self.assertIsNotNone(result["parse_error"])
        self.assertIn("ValueError", result["parse_error"])
        self.assertEqual(result["as_path"], [])

    def test_parse_bgp_update_truncated_payload_sets_parse_error(self) -> None:
        # 4-byte payload is too short to contain a valid BGP UPDATE header
        result = parse_bgp.parse_bgp_update_payload("deadbeef")
        self.assertIsNotNone(result["parse_error"])
        self.assertIn("truncated", result["parse_error"])

    def test_parse_single_event_non_update_returns_none(self) -> None:
        raw = json.dumps(
            {
                "event_type": "bgp",
                "bgp": {"message_type": "keepalive"},
            }
        )

        self.assertIsNone(parse_bgp.parse_single_event(raw))

    def test_parse_single_event_has_parse_error_field(self) -> None:
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
        self.assertIsNone(result["bgp"]["parse_error"])

    def test_parse_single_event_includes_asn_info(self) -> None:
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
        asn_info = result["bgp"]["asn_info"]
        # AS 65200 and 65000 are both private ASNs
        self.assertEqual(len(asn_info), 2)
        self.assertEqual(asn_info[0], {"asn": 65200, "handle": "AS65200", "description": "Private AS", "country_code": ""})
        self.assertEqual(asn_info[1], {"asn": 65000, "handle": "AS65000", "description": "Private AS", "country_code": ""})

    # ── VLAN alias ────────────────────────────────────────────

    def _make_mock_resp(self, body: bytes) -> MagicMock:
        resp = MagicMock()
        resp.__enter__ = lambda s: s
        resp.__exit__ = MagicMock(return_value=False)
        resp.read.return_value = body
        return resp

    def test_query_vlan_alias_returns_stripped_response(self) -> None:
        with patch("urllib.request.urlopen", return_value=self._make_mock_resp(b"core-sw\n")) as mock_open:
            result = parse_bgp._query_vlan_alias([104], "http://vlan_map_alias")
            mock_open.assert_called_once_with("http://vlan_map_alias?vlan=104", timeout=5)
            self.assertEqual(result, "core-sw")

    def test_query_vlan_alias_empty_list_returns_default(self) -> None:
        result = parse_bgp._query_vlan_alias([], "http://vlan_map_alias", default_alias="fallback")
        self.assertEqual(result, "fallback")

    def test_query_vlan_alias_no_url_returns_default(self) -> None:
        result = parse_bgp._query_vlan_alias([104], "", default_alias="fallback")
        self.assertEqual(result, "fallback")

    def test_query_vlan_alias_http_error_returns_default(self) -> None:
        with patch("urllib.request.urlopen", side_effect=Exception("connection refused")):
            result = parse_bgp._query_vlan_alias([104], "http://vlan_map_alias", default_alias="fallback")
            self.assertEqual(result, "fallback")

    def test_query_vlan_alias_empty_response_returns_default(self) -> None:
        with patch("urllib.request.urlopen", return_value=self._make_mock_resp(b"")):
            result = parse_bgp._query_vlan_alias([104], "http://vlan_map_alias", default_alias="fallback")
            self.assertEqual(result, "fallback")

    def test_parse_single_event_includes_alias_field(self) -> None:
        raw = json.dumps(
            {
                "event_type": "bgp",
                "vlan": [104],
                "bgp": {
                    "message_type": "update",
                    "payload": "00000024400101005002000a02020000feb00000fde8400304c0a83202c00808007b01c80141028e100a14",
                },
            }
        )

        with patch("urllib.request.urlopen", return_value=self._make_mock_resp(b"core-sw")):
            result = parse_bgp.parse_single_event(raw, vlan_map_alias_url="http://vlan_map_alias")

        self.assertIsNotNone(result)
        self.assertEqual(result["alias"], "core-sw")

    def test_parse_single_event_alias_uses_default_when_no_vlan(self) -> None:
        raw = json.dumps(
            {
                "event_type": "bgp",
                "bgp": {
                    "message_type": "update",
                    "payload": "00000024400101005002000a02020000feb00000fde8400304c0a83202c00808007b01c80141028e100a14",
                },
            }
        )

        result = parse_bgp.parse_single_event(
            raw,
            vlan_map_alias_url="http://vlan_map_alias",
            default_alias="default-alias",
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["alias"], "default-alias")

    # ── ASN info ──────────────────────────────────────────────

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
