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


_FTP_KWARGS = dict(
    ftp_hostname="ftp.example.com",
    ftp_user="ftpuser",
    ftp_password="ftppass",
    ftp_remote_path="/uploads",
)


def _make_mock_df(empty: bool = False, num_partitions: int = 2) -> MagicMock:
    df = MagicMock()
    df.isEmpty.return_value = empty
    df.rdd.getNumPartitions.return_value = num_partitions
    df.foreachPartition = MagicMock()
    return df


class TestWriteBatchToFtp(unittest.TestCase):

    def test_empty_batch_skips(self) -> None:
        df = _make_mock_df(empty=True)
        writer.write_batch_to_ftp(df, 0, **_FTP_KWARGS)
        df.foreachPartition.assert_not_called()

    def test_uses_foreach_partition_not_collect(self) -> None:
        df = _make_mock_df()
        writer.write_batch_to_ftp(df, 1, **_FTP_KWARGS)
        df.foreachPartition.assert_called_once()
        df.toJSON.assert_not_called()

    def test_partition_fn_uploads_rows_as_jsonl(self) -> None:
        df = _make_mock_df()
        writer.write_batch_to_ftp(df, 1, **_FTP_KWARGS)

        partition_fn = df.foreachPartition.call_args[0][0]

        mock_row = MagicMock()
        mock_row.asDict.return_value = {"src_ip": "1.2.3.4", "dest_ip": "5.6.7.8"}

        mock_ftp = MagicMock()
        mock_ftp.__enter__ = lambda s: s
        mock_ftp.__exit__ = MagicMock(return_value=False)

        with patch("ftplib.FTP", return_value=mock_ftp):
            partition_fn([mock_row])

        mock_ftp.login.assert_called_once_with(user="ftpuser", passwd="ftppass")
        cmd, buf = mock_ftp.storbinary.call_args[0]
        self.assertTrue(cmd.startswith("STOR /uploads/"))
        content = buf.read().decode()
        self.assertIn('"src_ip": "1.2.3.4"', content)

    def test_partition_fn_skips_empty_partition(self) -> None:
        df = _make_mock_df()
        writer.write_batch_to_ftp(df, 1, **_FTP_KWARGS)

        partition_fn = df.foreachPartition.call_args[0][0]

        with patch("ftplib.FTP") as mock_ftp_cls:
            partition_fn([])
            mock_ftp_cls.assert_not_called()

    def test_partition_fn_each_partition_gets_unique_filename(self) -> None:
        df = _make_mock_df()
        writer.write_batch_to_ftp(df, 1, **_FTP_KWARGS)

        partition_fn = df.foreachPartition.call_args[0][0]

        mock_row = MagicMock()
        mock_row.asDict.return_value = {"x": 1}

        uploaded_files = []

        def capture_stor(cmd, _buf):
            uploaded_files.append(cmd)

        mock_ftp = MagicMock()
        mock_ftp.__enter__ = lambda s: s
        mock_ftp.__exit__ = MagicMock(return_value=False)
        mock_ftp.storbinary.side_effect = capture_stor

        with patch("ftplib.FTP", return_value=mock_ftp):
            partition_fn([mock_row])
            partition_fn([mock_row])

        self.assertEqual(len(uploaded_files), 2)
        self.assertNotEqual(uploaded_files[0], uploaded_files[1])


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
