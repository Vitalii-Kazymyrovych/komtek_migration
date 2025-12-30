#!/usr/bin/env python3
"""Streams migration helper.

Reads legacy streams data, normalizes text fields, remaps foreign keys using
existing mappings, and regenerates:
  - new_dataset/streams_202512301039.txt
  - sql/streams_inserts.sql
  - maps/streams.json
"""
from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


OLD_STREAMS_PATH = Path("old_dataset/_streams__202512301049.txt")
NEW_STREAMS_PATH = Path("new_dataset/streams_202512301039.txt")
CLIENT_MAP_PATH = Path("maps/clients.json")
STREAM_GROUP_MAP_PATH = Path("maps/stream_groups.json")
USER_MAP_PATH = Path("maps/users.json")
SQL_OUTPUT_PATH = Path("sql/streams_inserts.sql")
MAP_OUTPUT_PATH = Path("maps/streams.json")

SUBSTITUTIONS = str.maketrans(
    {
        "ß": "ss",
        "æ": "ae",
        "Æ": "AE",
        "ø": "o",
        "Ø": "O",
        "đ": "d",
        "Đ": "D",
        "ł": "l",
        "Ł": "L",
    }
)


@dataclass
class StreamRecord:
    """Normalized stream record with remapped references."""

    id: int
    old_id: int
    name: str
    path: Optional[str]
    width: Optional[int]
    height: Optional[int]
    file_name: Optional[str]
    status: int
    created_at: Optional[str]
    lat: Optional[str]
    lng: Optional[str]
    type: Optional[str]
    uuid: Optional[str]
    address: Optional[str]
    params: Optional[Dict[str, Any]]
    auth: Optional[Dict[str, Any]]
    direction: Optional[int]
    client_id: int
    old_client_id: int
    codec: Optional[str]
    timezone: Optional[str]
    duration: Optional[int]
    restrictions: Optional[Dict[str, Any]]
    old_creator_id: Optional[int]
    new_creator_id: Optional[int]
    parent_id: int
    old_parent_id: int


def normalize_text(value: Optional[str]) -> Optional[str]:
    """Normalize text to ASCII, stripping whitespace and diacritics."""
    if value is None:
        return None

    trimmed = value.strip()
    if trimmed in {"", "-", "NULL", "null", "[NULL]"}:
        return None

    substituted = trimmed.translate(SUBSTITUTIONS)
    normalized = unicodedata.normalize("NFKD", substituted)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return ascii_text


def clean_value(value: Optional[str]) -> Optional[str]:
    """Return a trimmed value or None when empty/null placeholders are present."""
    if value is None:
        return None

    trimmed = value.strip()
    if trimmed in {"", "-", "NULL", "null", "[NULL]"}:
        return None
    return trimmed


def strip_outer_quotes(value: Optional[str]) -> Optional[str]:
    """Remove one layer of surrounding double quotes if present."""
    if value is None:
        return None
    stripped = value.strip()
    if stripped.startswith('"') and stripped.endswith('"') and len(stripped) >= 2:
        return stripped[1:-1]
    return stripped


def to_int(value: Optional[str]) -> Optional[int]:
    """Convert a numeric-looking string to int, removing thousands separators."""
    cleaned = clean_value(value)
    if cleaned is None:
        return None
    numeric = cleaned.replace(",", "")
    return int(numeric)


def parse_json_field(value: Optional[str]) -> Optional[Dict[str, Any]]:
    """Parse a JSON payload that may be double-quoted and escape-encoded."""
    cleaned = clean_value(value)
    if cleaned is None:
        return None

    unquoted = strip_outer_quotes(cleaned)
    decoded = unquoted.encode("utf-8").decode("unicode_escape")
    try:
        return json.loads(decoded)
    except json.JSONDecodeError:
        return json.loads(unquoted)


def parse_pipe_table(path: Path) -> List[Dict[str, Optional[str]]]:
    """Parse a pipe-delimited table file with a header row."""
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 2:
        return []

    headers = [header.strip() for header in lines[0].strip("|").split("|")]
    data_rows = []
    for line in lines[2:]:
        if not line.strip():
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        data_rows.append(dict(zip(headers, cells)))
    return data_rows


def load_id_map(path: Path) -> Dict[int, int]:
    """Load an old->new id mapping from an existing mapping file."""
    mapping_data = json.loads(path.read_text(encoding="utf-8"))
    return {entry["old_id"]: entry["new_id"] for entry in mapping_data.get("mapped", [])}


def build_records(
    rows: List[Dict[str, Optional[str]]],
    client_map: Dict[int, int],
    stream_group_map: Dict[int, int],
    user_map: Dict[int, int],
) -> tuple[List[StreamRecord], List[Dict[str, Any]]]:
    """Normalize and remap rows, preserving deterministic ordering.

    Returns a tuple of (mapped_records, unmapped_old_rows).
    """
    records: List[StreamRecord] = []
    unmapped_old: List[Dict[str, Any]] = []

    for row in rows:
        status = to_int(row.get("status"))
        if status == -1:
            unmapped_old.append(
                {
                    "old_id": to_int(row.get("id")),
                    "name": normalize_text(row.get("name")) or row.get("name"),
                    "old_client_id": to_int(row.get("client_id")),
                    "old_parent_id": to_int(row.get("parent_id")),
                    "reason": "status = -1 (excluded)",
                }
            )
            continue

        client_id = to_int(row.get("client_id"))
        parent_id = to_int(row.get("parent_id")) or 0
        if client_id is None or client_id not in client_map:
            unmapped_old.append(
                {
                    "old_id": to_int(row.get("id")),
                    "name": normalize_text(row.get("name")) or row.get("name"),
                    "old_client_id": client_id,
                    "old_parent_id": parent_id,
                    "reason": "client_id missing from clients mapping",
                }
            )
            continue

        if parent_id not in (0,) and parent_id not in stream_group_map:
            unmapped_old.append(
                {
                    "old_id": to_int(row.get("id")),
                    "name": normalize_text(row.get("name")) or row.get("name"),
                    "old_client_id": client_id,
                    "old_parent_id": parent_id,
                    "reason": "parent_id missing from stream_groups mapping",
                }
            )
            continue

        normalized_name = normalize_text(row.get("name"))
        if normalized_name is None:
            unmapped_old.append(
                {
                    "old_id": to_int(row.get("id")),
                    "old_client_id": client_id,
                    "old_parent_id": parent_id,
                    "reason": "name missing after normalization",
                }
            )
            continue

        restrictions = parse_json_field(row.get("restrictions"))
        old_creator_id = restrictions.get("creator_id") if restrictions else None
        new_creator_id = user_map.get(old_creator_id) if old_creator_id is not None else None
        if restrictions is not None:
            restrictions = {**restrictions, "creator_id": new_creator_id}

        record = StreamRecord(
            id=to_int(row.get("id")) or 0,
            old_id=to_int(row.get("id")) or 0,
            name=normalized_name,
            path=normalize_text(strip_outer_quotes(row.get("path"))),
            width=to_int(row.get("width")),
            height=to_int(row.get("height")),
            file_name=normalize_text(strip_outer_quotes(row.get("file_name"))),
            status=status or 0,
            created_at=clean_value(row.get("created_at")),
            lat=clean_value(row.get("lat")),
            lng=clean_value(row.get("lng")),
            type=normalize_text(row.get("type")),
            uuid=normalize_text(row.get("uuid")),
            address=normalize_text(strip_outer_quotes(row.get("address"))) or "",
            params=parse_json_field(row.get("params")),
            auth=parse_json_field(row.get("auth")),
            direction=to_int(row.get("direction")),
            client_id=client_map[client_id],
            old_client_id=client_id,
            codec=normalize_text(row.get("codec")),
            timezone=normalize_text(row.get("timezone")),
            duration=to_int(row.get("duration")),
            restrictions=restrictions,
            old_creator_id=old_creator_id,
            new_creator_id=new_creator_id,
            parent_id=stream_group_map.get(parent_id, 0),
            old_parent_id=parent_id,
        )
        records.append(record)

    records.sort(key=lambda record: record.id)
    unmapped_old.sort(key=lambda record: (record["old_id"] or 0))
    return records, unmapped_old


def format_json_field(payload: Optional[Dict[str, Any]]) -> str:
    """Serialize JSON payloads for dataset/SQL output."""
    if payload is None:
        return "[NULL]"
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def format_cell(value: Optional[Any]) -> str:
    """Serialize a dataset cell to pipe-table format."""
    if value is None:
        return "[NULL]"
    return str(value)


def write_new_dataset(records: List[StreamRecord], path: Path) -> None:
    """Write the normalized streams table to the new dataset file."""
    lines = [
        "|id |name|path|width|height|file_name|status|created_at|lat|lng|type|uuid|address|params|auth|direction|client_id|codec|timezone|duration|restrictions|parent_id|",
        "|---|----|----|-----|------|---------|------|----------|---|---|----|----|-------|------|----|---------|---------|-----|--------|--------|------------|---------|",
    ]
    for record in records:
        lines.append(
            "|".join(
                [
                    f"|{format_cell(record.id)}",
                    format_cell(record.name),
                    format_cell(record.path),
                    format_cell(record.width),
                    format_cell(record.height),
                    format_cell(record.file_name),
                    format_cell(record.status),
                    format_cell(record.created_at),
                    format_cell(record.lat),
                    format_cell(record.lng),
                    format_cell(record.type),
                    format_cell(record.uuid),
                    format_cell(record.address),
                    format_json_field(record.params),
                    format_json_field(record.auth),
                    format_cell(record.direction),
                    format_cell(record.client_id),
                    format_cell(record.codec),
                    format_cell(record.timezone),
                    format_cell(record.duration),
                    format_json_field(record.restrictions),
                    f"{format_cell(record.parent_id)}|",
                ]
            )
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def sql_string(value: Optional[str]) -> str:
    """Escape a string for SQL output."""
    if value is None:
        return "NULL"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def sql_json(payload: Optional[Dict[str, Any]]) -> str:
    """Format JSON payloads as SQL string literals."""
    if payload is None:
        return "NULL"
    return sql_string(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))


def sql_numeric(value: Optional[Any]) -> str:
    """Format numeric values for SQL output."""
    if value is None:
        return "NULL"
    return str(value)


def write_sql(records: List[StreamRecord], path: Path) -> None:
    """Generate a batched INSERT statement for streams."""
    value_lines = []
    for record in records:
        value_lines.append(
            "  ("
            + ", ".join(
                [
                    sql_numeric(record.id),
                    sql_string(record.name),
                    sql_string(record.path),
                    sql_numeric(record.width),
                    sql_numeric(record.height),
                    sql_string(record.file_name),
                    sql_numeric(record.status),
                    sql_string(record.created_at),
                    sql_numeric(record.lat),
                    sql_numeric(record.lng),
                    sql_string(record.type),
                    sql_string(record.uuid),
                    sql_string(record.address),
                    sql_json(record.params),
                    sql_json(record.auth),
                    sql_numeric(record.direction),
                    sql_numeric(record.client_id),
                    sql_string(record.codec),
                    sql_string(record.timezone),
                    sql_numeric(record.duration),
                    sql_json(record.restrictions),
                    sql_numeric(record.parent_id),
                ]
            )
            + ")"
        )

    sql = (
        'INSERT INTO videoanalytics.streams (id, "name", path, width, height, file_name, status, created_at, lat, lng, type, uuid, address, params, auth, direction, client_id, codec, timezone, duration, restrictions, parent_id)\nVALUES\n'
        + ",\n".join(value_lines)
        + ";\n"
    )
    path.write_text(sql, encoding="utf-8")


def write_mapping(
    records: List[StreamRecord], unmapped_old: List[Dict[str, Any]], path: Path
) -> None:
    """Create the mapping JSON artifact."""
    mapping = {
        "match_keys": [
            "id preserved 1:1 to id",
            "name (ASCII normalized) to name",
            "client_id remapped via clients.json to client_id",
            "parent_id remapped via stream_groups.json to parent_id",
            "restrictions.creator_id remapped via users.json to restrictions.creator_id",
        ],
        "mapped": [
            {
                "old_id": record.old_id,
                "new_id": record.id,
                "name": record.name,
                "old_client_id": record.old_client_id,
                "new_client_id": record.client_id,
                "old_parent_id": record.old_parent_id,
                "new_parent_id": record.parent_id,
                "old_creator_id": record.old_creator_id,
                "new_creator_id": record.new_creator_id,
                "status": record.status,
            }
            for record in records
        ],
        "unmapped_old": unmapped_old,
        "unmapped_new": [],
    }
    path.write_text(json.dumps(mapping, indent=2), encoding="utf-8")


def main() -> None:
    client_map = load_id_map(CLIENT_MAP_PATH)
    stream_group_map = load_id_map(STREAM_GROUP_MAP_PATH)
    user_map = load_id_map(USER_MAP_PATH)
    legacy_rows = parse_pipe_table(OLD_STREAMS_PATH)
    records, unmapped_old = build_records(legacy_rows, client_map, stream_group_map, user_map)

    write_new_dataset(records, NEW_STREAMS_PATH)
    write_sql(records, SQL_OUTPUT_PATH)
    write_mapping(records, unmapped_old, MAP_OUTPUT_PATH)
    print(f"Processed {len(records)} mapped streams, {len(unmapped_old)} unmapped.")


if __name__ == "__main__":
    main()
