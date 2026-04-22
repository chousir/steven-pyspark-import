"""Utilities for parsing Suricata BGP UPDATE events in Spark."""

from __future__ import annotations

import csv
import ipaddress
import json
import urllib.request
from functools import lru_cache
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


EVE_BGP_SCHEMA = T.StructType(
	[
		T.StructField("timestamp", T.StringType(), True),
		T.StructField("flow_id", T.LongType(), True),
		T.StructField("pcap_cnt", T.LongType(), True),
		T.StructField("event_type", T.StringType(), True),
		T.StructField("src_ip", T.StringType(), True),
		T.StructField("src_port", T.IntegerType(), True),
		T.StructField("dest_ip", T.StringType(), True),
		T.StructField("dest_port", T.IntegerType(), True),
		T.StructField("proto", T.StringType(), True),
		T.StructField("ip_v", T.IntegerType(), True),
		T.StructField("pkt_src", T.StringType(), True),
        T.StructField("vlan", T.ArrayType(T.IntegerType()), True),
		T.StructField(
			"bgp",
			T.StructType(
				[
					T.StructField("message_type", T.StringType(), True),
					T.StructField("payload_length", T.IntegerType(), True),
					T.StructField("payload", T.StringType(), True),
				]
			),
			True,
		),
	]
)


BGP_PARSE_SCHEMA = T.StructType(
	[
		T.StructField("as_path", T.ArrayType(T.LongType()), True),
		T.StructField("next_hop", T.StringType(), True),
		T.StructField("nlri", T.ArrayType(T.StringType()), True),
		T.StructField("withdrawn", T.ArrayType(T.StringType()), True),
		T.StructField("parse_error", T.StringType(), True),
	]
)


ASN_ORG_SCHEMA = T.StructType(
	[
		T.StructField("asn", T.LongType(), True),
		T.StructField("handle", T.StringType(), True),
		T.StructField("description", T.StringType(), True),
		T.StructField("country_code", T.StringType(), True),
	]
)


_AS_INFO_SCHEMA = T.ArrayType(
	T.StructType(
		[
			T.StructField("asn", T.LongType(), True),
			T.StructField("handle", T.StringType(), True),
			T.StructField("description", T.StringType(), True),
			T.StructField("country_code", T.StringType(), True),
		]
	)
)


def _read_u16(data: bytes, offset: int) -> tuple[int, int]:
	if offset + 2 > len(data):
		raise ValueError("not enough bytes for u16")
	return int.from_bytes(data[offset : offset + 2], byteorder="big"), offset + 2


def _read_u32(data: bytes, offset: int) -> tuple[int, int]:
	if offset + 4 > len(data):
		raise ValueError("not enough bytes for u32")
	return int.from_bytes(data[offset : offset + 4], byteorder="big"), offset + 4


def _parse_prefix_list(data: bytes) -> list[str]:
	prefixes: list[str] = []
	offset = 0

	while offset < len(data):
		prefix_len = data[offset]
		offset += 1

		if prefix_len > 32:
			break

		octets = (prefix_len + 7) // 8
		if offset + octets > len(data):
			break

		raw = data[offset : offset + octets]
		offset += octets

		padded = raw + (b"\x00" * (4 - len(raw)))
		network = f"{ipaddress.IPv4Address(padded)}/{prefix_len}"
		prefixes.append(network)

	return prefixes


def _parse_prefix_list_v6(data: bytes) -> list[str]:
	prefixes: list[str] = []
	offset = 0

	while offset < len(data):
		prefix_len = data[offset]
		offset += 1

		if prefix_len > 128:
			break

		octets = (prefix_len + 7) // 8
		if offset + octets > len(data):
			break

		raw = data[offset : offset + octets]
		offset += octets

		padded = raw + (b"\x00" * (16 - len(raw)))
		network = f"{ipaddress.IPv6Address(padded)}/{prefix_len}"
		prefixes.append(network)

	return prefixes


def _can_parse_as_path_with_size(value: bytes, asn_size: int) -> bool:
	offset = 0

	while offset < len(value):
		if offset + 2 > len(value):
			return False

		segment_len = value[offset + 1]
		offset += 2

		needed = segment_len * asn_size
		if offset + needed > len(value):
			return False

		offset += needed

	return offset == len(value)


def _detect_asn_byte_size(value: bytes) -> int:
	can_parse_2 = _can_parse_as_path_with_size(value, 2)
	can_parse_4 = _can_parse_as_path_with_size(value, 4)

	if can_parse_2 and not can_parse_4:
		return 2
	if can_parse_4 and not can_parse_2:
		return 4
	if can_parse_2 and can_parse_4:
		# Ambiguous encoding; prefer 2-byte for broad compatibility.
		return 2

	# Fallback so parser can still attempt to extract something.
	return 2


def _parse_as_path_attr(value: bytes, asn_size: int) -> list[int]:
	as_path: list[int] = []
	offset = 0

	while offset + 2 <= len(value):
		_segment_type = value[offset]
		segment_len = value[offset + 1]
		offset += 2

		needed = segment_len * asn_size
		if offset + needed > len(value):
			break

		for _ in range(segment_len):
			if asn_size == 4:
				asn, offset = _read_u32(value, offset)
			else:
				asn, offset = _read_u16(value, offset)
			as_path.append(asn)

	return as_path


def _is_private_asn(asn: int) -> bool:
	# RFC 6996 private-use ASN ranges.
	return (64512 <= asn <= 65534) or (4200000000 <= asn <= 4294967294)


def _asn_info(asn: int, asn_map: dict[int, tuple[str, str, str]]) -> tuple[str, str, str]:
	if asn in asn_map:
		return asn_map[asn]
	if _is_private_asn(asn):
		return (f"AS{asn}", "Private AS", "")
	return (f"AS{asn}", f"AS{asn}", "")


@lru_cache(maxsize=4)
def _load_asn_org_map(asn_csv_path: str) -> dict[int, tuple[str, str, str]]:
	asn_map: dict[int, tuple[str, str, str]] = {}
	try:
		with open(asn_csv_path, newline="", encoding="utf-8") as csv_file:
			reader = csv.DictReader(csv_file)
			for row in reader:
				asn_raw = row.get("asn")
				if asn_raw is None:
					continue
				try:
					asn_map[int(asn_raw)] = (
						row.get("handle") or "",
						row.get("description") or "",
						row.get("country-code") or "",
					)
				except ValueError:
					continue
	except OSError:
		return {}
	return asn_map


def parse_bgp_update_payload(payload_hex: str | None) -> dict[str, Any]:
	"""
	Parse BGP UPDATE payload hex string.

	Returns a dict with keys: as_path, next_hop, nlri, parse_error.
	parse_error is None on success or when payload is absent (legitimate empty).
	On any failure it contains a descriptive string so callers can monitor
	parse failure rates without data loss.
	"""
	result: dict[str, Any] = {"as_path": [], "next_hop": None, "nlri": [], "withdrawn": [], "parse_error": None}

	if not payload_hex:
		return result

	try:
		payload = bytes.fromhex(payload_hex)
	except ValueError as exc:
		result["parse_error"] = f"ValueError: {exc}"
		return result

	try:
		offset = 0
		as_path_type2: list[int] | None = None
		as_path_type17: list[int] | None = None

		withdrawn_len, offset = _read_u16(payload, offset)
		if offset + withdrawn_len > len(payload):
			result["parse_error"] = "truncated: withdrawn routes overflow packet boundary"
			return result

		result["withdrawn"] = _parse_prefix_list(payload[offset : offset + withdrawn_len])
		offset += withdrawn_len

		path_attr_len, offset = _read_u16(payload, offset)
		path_attr_end = offset + path_attr_len
		if path_attr_end > len(payload):
			result["parse_error"] = "truncated: path attributes overflow packet boundary"
			return result

		while offset < path_attr_end:
			if offset + 2 > path_attr_end:
				break

			flags = payload[offset]
			attr_type = payload[offset + 1]
			offset += 2

			extended_len = bool(flags & 0x10)
			if extended_len:
				attr_len, offset = _read_u16(payload, offset)
			else:
				if offset + 1 > path_attr_end:
					break
				attr_len = payload[offset]
				offset += 1

			if offset + attr_len > path_attr_end:
				break

			attr_value = payload[offset : offset + attr_len]
			offset += attr_len

			if attr_type == 2:
				asn_size = _detect_asn_byte_size(attr_value)
				as_path_type2 = _parse_as_path_attr(attr_value, asn_size)
			elif attr_type == 17:
				# AS4_PATH is always encoded with 4-byte ASNs.
				as_path_type17 = _parse_as_path_attr(attr_value, 4)
			elif attr_type == 3 and len(attr_value) == 4:
				result["next_hop"] = str(ipaddress.IPv4Address(attr_value))
			elif attr_type == 14:
				# MP_REACH_NLRI: AFI(2) + SAFI(1) + NH_LEN(1) + NH + SNPA(1) + NLRI
				if len(attr_value) < 4:
					continue
				afi = int.from_bytes(attr_value[0:2], "big")
				nh_len = attr_value[3]
				if 4 + nh_len > len(attr_value):
					continue
				nh_bytes = attr_value[4 : 4 + nh_len]
				if afi == 2 and nh_len == 16:
					result["next_hop"] = str(ipaddress.IPv6Address(nh_bytes))
				elif afi == 1 and nh_len == 4:
					result["next_hop"] = str(ipaddress.IPv4Address(nh_bytes))
				snpa_offset = 4 + nh_len + 1  # skip SNPA count byte
				if snpa_offset <= len(attr_value):
					nlri_data = attr_value[snpa_offset:]
					if afi == 2:
						result["nlri"] = _parse_prefix_list_v6(nlri_data)
					else:
						result["nlri"] = _parse_prefix_list(nlri_data)

		if as_path_type17 is not None:
			result["as_path"] = as_path_type17
		elif as_path_type2 is not None:
			result["as_path"] = as_path_type2

		if not result["nlri"]:
			result["nlri"] = _parse_prefix_list(payload[path_attr_end:])
		return result
	except Exception as exc:
		result["parse_error"] = f"{type(exc).__name__}: {exc}"
		return result


_parse_bgp_update_udf = F.udf(parse_bgp_update_payload, BGP_PARSE_SCHEMA)


def _query_vlan_alias(
	vlan_list: list[int] | None,
	vlan_map_alias_url: str,
	default_alias: str = "",
) -> str:
	"""Fetch alias for the first VLAN in *vlan_list* from {vlan_map_alias_url}?vlan=<id>.

	Returns *default_alias* when vlan_list is empty, the URL is not configured,
	or the HTTP request fails.
	"""
	if not vlan_list or not vlan_map_alias_url:
		return default_alias
	try:
		url = f"{vlan_map_alias_url}?vlan={vlan_list[0]}"
		with urllib.request.urlopen(url, timeout=5) as resp:
			result = resp.read().decode("utf-8").strip()
			return result if result else default_alias
	except Exception:
		return default_alias


def parse_bgp_updates(
	df: DataFrame,
	value_col: str = "value",
	asn_csv_path: str = "as.csv",
	vlan_map_alias_url: str = "",
	default_alias: str = "",
) -> DataFrame:
	"""
	Parse Suricata eve.json records and keep only BGP UPDATE events.

	Input:
	- `df` must contain a JSON string column (default `value`).
	- `asn_csv_path`: path to as-metadata CSV for ASN → Organization lookup.
	- `vlan_map_alias_url`: full URL used to resolve VLAN aliases (e.g. ``http://vlan_map_alias``).
	- `default_alias`: fallback value for `alias` when VLAN is absent or lookup fails.

	Output:
	- One row per BGP UPDATE event.
	- `bgp` struct contains `message_type`, `asn_info` (array of structs with asn/handle/description/country_code), `next_hop`, `nlri`.
	- `alias`: VLAN alias resolved via ``vlan_map_alias_url``; falls back to ``default_alias``.
	"""
	parsed = (
		df.withColumn("json", F.from_json(F.col(value_col), EVE_BGP_SCHEMA))
		.select("json.*")
		.where((F.col("event_type") == F.lit("bgp")) & (F.col("bgp.message_type") == F.lit("update")))
	)

	parsed_fields = _parse_bgp_update_udf(F.col("bgp.payload"))

	# Load ASN → (handle, description, country-code) mapping
	spark = df.sparkSession
	asn_df = spark.read.csv(
		asn_csv_path,
		header=True,
		schema=ASN_ORG_SCHEMA,
	)
	asn_dict = {
		row.asn: (row.handle or "", row.description or "", row.country_code or "")
		for row in asn_df.collect()
		if row.asn is not None
	}
	asn_dict_broadcast = spark.sparkContext.broadcast(asn_dict)

	def _map_asn_to_info(asn_list: list[int]) -> list[dict]:
		if not asn_list:
			return []
		asn_map = asn_dict_broadcast.value
		result = []
		for asn in asn_list:
			h, d, c = _asn_info(asn, asn_map)
			result.append({"asn": asn, "handle": h, "description": d, "country_code": c})
		return result

	_map_asn_udf = F.udf(_map_asn_to_info, _AS_INFO_SCHEMA)

	_alias_url = vlan_map_alias_url
	_alias_default = default_alias

	def _fetch_alias(vlan_list: list[int]) -> str:
		return _query_vlan_alias(vlan_list, _alias_url, _alias_default)

	_fetch_alias_udf = F.udf(_fetch_alias, T.StringType())

	return (
		parsed.withColumn("parsed_bgp", parsed_fields)
		.withColumn("as_info", _map_asn_udf(F.col("parsed_bgp.as_path")))
		.withColumn(
			"bgp",
			F.struct(
				F.col("bgp.message_type").alias("message_type"),
				F.col("as_info").alias("asn_info"),
				F.col("parsed_bgp.next_hop").alias("next_hop"),
				F.col("parsed_bgp.nlri").alias("nlri"),
				F.col("parsed_bgp.withdrawn").alias("withdrawn"),
				F.col("parsed_bgp.parse_error").alias("parse_error"),
				F.col("bgp.payload").alias("payload"),
			),
		)
		.withColumn("alias", _fetch_alias_udf(F.col("vlan")))
		.drop("parsed_bgp", "as_info")
	)


def parse_single_event(
	raw_json: str,
	asn_csv_path: str = "as.csv",
	vlan_map_alias_url: str = "",
	default_alias: str = "",
) -> dict[str, Any] | None:
	"""Parse one eve.json string into output shape used by parse_bgp_updates."""
	try:
		event = json.loads(raw_json)
	except json.JSONDecodeError:
		return None

	if event.get("event_type") != "bgp":
		return None

	bgp = event.get("bgp") or {}
	if bgp.get("message_type") != "update":
		return None

	parsed = parse_bgp_update_payload(bgp.get("payload"))
	asn_map = _load_asn_org_map(asn_csv_path)
	asn_info = []
	for asn in parsed["as_path"]:
		h, d, c = _asn_info(asn, asn_map)
		asn_info.append({"asn": asn, "handle": h, "description": d, "country_code": c})
	event["bgp"] = {
		"message_type": "update",
		"asn_info": asn_info,
		"next_hop": parsed["next_hop"],
		"nlri": parsed["nlri"],
		"withdrawn": parsed["withdrawn"],
		"parse_error": parsed["parse_error"],
		"payload": bgp.get("payload"),
	}
	event["alias"] = _query_vlan_alias(event.get("vlan"), vlan_map_alias_url, default_alias)
	return event
