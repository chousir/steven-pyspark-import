import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch


def _install_pyspark_stubs() -> None:
    pyspark_mod = types.ModuleType("pyspark")
    sql_mod = types.ModuleType("pyspark.sql")
    storage_mod = types.ModuleType("pyspark.storagelevel")

    class DataFrame:  # pragma: no cover
        pass

    class StorageLevel:  # pragma: no cover
        MEMORY_AND_DISK = None

    sql_mod.DataFrame = DataFrame
    storage_mod.StorageLevel = StorageLevel

    sys.modules.setdefault("pyspark", pyspark_mod)
    sys.modules.setdefault("pyspark.sql", sql_mod)
    sys.modules.setdefault("pyspark.storagelevel", storage_mod)


_install_pyspark_stubs()

MODULE_PATH = Path(__file__).resolve().parents[1] / "pyspark-import" / "writer.py"
SPEC = importlib.util.spec_from_file_location("writer", MODULE_PATH)
writer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(writer)


_ES_KWARGS = dict(
    es_user="admin",
    es_password="secret",
    es_url="es.example.com",
    es_port="9200",
    es_index="bgp-*",
)


def _make_mock_resp(status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    resp.status = status
    return resp


class TestCreateEsIndexTemplate(unittest.TestCase):

    def test_puts_to_correct_url(self) -> None:
        with patch("urllib.request.urlopen", return_value=_make_mock_resp()) as mock_open:
            writer.create_es_index_template(**_ES_KWARGS)

        req = mock_open.call_args[0][0]
        self.assertEqual(req.full_url, "https://es.example.com:9200/_index_template/bgp-template")
        self.assertEqual(req.get_method(), "PUT")

    def test_sends_basic_auth_header(self) -> None:
        import base64

        with patch("urllib.request.urlopen", return_value=_make_mock_resp()) as mock_open:
            writer.create_es_index_template(**_ES_KWARGS)

        req = mock_open.call_args[0][0]
        expected = "Basic " + base64.b64encode(b"admin:secret").decode()
        self.assertEqual(req.get_header("Authorization"), expected)

    def test_template_body_contains_ip_mappings(self) -> None:
        with patch("urllib.request.urlopen", return_value=_make_mock_resp()) as mock_open:
            writer.create_es_index_template(**_ES_KWARGS)

        req = mock_open.call_args[0][0]
        body = json.loads(req.data.decode())

        self.assertEqual(body["index_patterns"], ["bgp-*"])
        props = body["template"]["mappings"]["properties"]
        self.assertEqual(props["src_ip"],  {"type": "ip"})
        self.assertEqual(props["dest_ip"], {"type": "ip"})
        self.assertEqual(props["bgp"]["properties"]["next_hop"], {"type": "ip"})
        self.assertEqual(props["bgp"]["properties"]["nlri"],     {"type": "ip_range"})

    def test_custom_template_name(self) -> None:
        with patch("urllib.request.urlopen", return_value=_make_mock_resp()) as mock_open:
            writer.create_es_index_template(**_ES_KWARGS, template_name="my-bgp-tmpl")

        req = mock_open.call_args[0][0]
        self.assertIn("my-bgp-tmpl", req.full_url)

    def test_http_error_raises_runtime_error(self) -> None:
        import urllib.error

        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.HTTPError(None, 403, "Forbidden", {}, None),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                writer.create_es_index_template(**_ES_KWARGS)

        self.assertIn("403", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
