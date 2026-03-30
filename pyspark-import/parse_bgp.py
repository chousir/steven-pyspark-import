"""Utilities for parsing Suricata BGP UPDATE events in Spark."""

from __future__ import annotations

import ipaddress
import json
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
		T.StructField("as_path", T.ArrayType(T.IntegerType()), True),
		T.StructField("next_hop", T.StringType(), True),
		T.StructField("nlri", T.ArrayType(T.StringType()), True),
	]
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

		octets = (prefix_len + 7) // 8
		if offset + octets > len(data):
			break

		raw = data[offset : offset + octets]
		offset += octets

		padded = raw + (b"\x00" * (4 - len(raw)))
		network = f"{ipaddress.IPv4Address(padded)}/{prefix_len}"
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


def parse_bgp_update_payload(payload_hex: str | None) -> dict[str, Any]:
	"""
	Parse BGP UPDATE payload hex string.

	Returns a dict with keys: as_path, next_hop, nlri.
	If parsing fails, returns empty values instead of raising.
	"""
	result: dict[str, Any] = {"as_path": [], "next_hop": None, "nlri": []}

	if not payload_hex:
		return result

	try:
		payload = bytes.fromhex(payload_hex)
	except ValueError:
		return result

	try:
		offset = 0
		as_path_type2: list[int] | None = None
		as_path_type17: list[int] | None = None

		withdrawn_len, offset = _read_u16(payload, offset)
		if offset + withdrawn_len > len(payload):
			return result

		offset += withdrawn_len

		path_attr_len, offset = _read_u16(payload, offset)
		path_attr_end = offset + path_attr_len
		if path_attr_end > len(payload):
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

		if as_path_type17 is not None:
			result["as_path"] = as_path_type17
		elif as_path_type2 is not None:
			result["as_path"] = as_path_type2

		result["nlri"] = _parse_prefix_list(payload[path_attr_end:])
		return result
	except Exception:
		return result


_parse_bgp_update_udf = F.udf(parse_bgp_update_payload, BGP_PARSE_SCHEMA)


def parse_bgp_updates(df: DataFrame, value_col: str = "value") -> DataFrame:
	"""
	Parse Suricata eve.json records and keep only BGP UPDATE events.

	Input:
	- `df` must contain a JSON string column (default `value`).

	Output:
	- One row per BGP UPDATE event.
	- `bgp` struct contains `message_type`, `as_path`, `next_hop`, `nlri`.
	"""
	parsed = (
		df.withColumn("json", F.from_json(F.col(value_col), EVE_BGP_SCHEMA))
		.select("json.*")
		.where((F.col("event_type") == F.lit("bgp")) & (F.col("bgp.message_type") == F.lit("update")))
	)

	parsed_fields = _parse_bgp_update_udf(F.col("bgp.payload"))

	return (
		parsed.withColumn("parsed_bgp", parsed_fields)
		.withColumn(
			"bgp",
			F.struct(
				F.col("bgp.message_type").alias("message_type"),
				F.col("parsed_bgp.as_path").alias("as_path"),
				F.col("parsed_bgp.next_hop").alias("next_hop"),
				F.col("parsed_bgp.nlri").alias("nlri"),
			),
		)
		.drop("parsed_bgp")
	)


def parse_single_event(raw_json: str) -> dict[str, Any] | None:
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
	event["bgp"] = {
		"message_type": "update",
		"as_path": parsed["as_path"],
		"next_hop": parsed["next_hop"],
		"nlri": parsed["nlri"],
	}
	return event
