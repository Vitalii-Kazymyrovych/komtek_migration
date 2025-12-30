#!/usr/bin/env python3
"""Face lists migration helper.

Normalizes legacy face list records, remaps references via existing mappings,
and regenerates:
  - new_dataset/face_lists_202512301039.txt
  - sql/face_lists_inserts.sql
  - maps/face_lists.json
"""
from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


OLD_FACE_LISTS_PATH = Path("old_dataset/_face_lists__202512301049.txt")
NEW_FACE_LISTS_PATH = Path("new_dataset/face_lists_202512301039.txt")
CLIENT_MAP_PATH = Path("maps/clients.json")
USER_MAP_PATH = Path("maps/users.json")
ANALYTICS_MAP_PATH = Path("maps/analytics.json")
SQL_FACE_LISTS_PATH = Path("sql/face_lists_inserts.sql")
MAP_FACE_LISTS_PATH = Path("maps/face_lists.json")


# Preserve all existing rows so pre-existing data stays untouched.
PRESERVE_FACE_LIST_IDS: Optional[set[int]] = None


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


PLACEHOLDER_NULLS = {"", "-", "NULL", "null", "[NULL]"}


@dataclass
class FaceListRecord:
    """Normalized face list row ready for dataset/SQL output."""

    id: int
    old_id: int
    name: str
    comment: Optional[str]
    min_confidence: int
    send_internal_notifications: bool
    events_holder: Optional[Dict[str, Any]]
    status: int
    created_at: Optional[str]
    client_id: int
    old_client_id: int
    color: str
    time_attendance: Optional[Dict[str, Any]]
    list_permissions: Dict[str, Any]
    analytics_ids: List[int]
    show_popup_for_internal_notifications: bool
    unmapped_stream_ids: List[int]


def normalize_text(value: Optional[str]) -> Optional[str]:
    """Normalize text to ASCII, stripping whitespace and diacritics."""
    if value is None:
        return None

    trimmed = value.strip()
    if trimmed in PLACEHOLDER_NULLS:
        return None

    substituted = trimmed.translate(SUBSTITUTIONS)
    normalized = unicodedata.normalize("NFKD", substituted)
    return normalized.encode("ascii", "ignore").decode("ascii")


def clean_value(value: Optional[str]) -> Optional[str]:
    """Return a trimmed value or None when empty/null placeholders are present."""
    if value is None:
        return None
    trimmed = value.strip()
    if trimmed in PLACEHOLDER_NULLS:
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


def parse_json_field(raw: Optional[str]) -> Optional[Any]:
    """Parse a JSON field that may be double-encoded."""
    if raw is None:
        return None
    candidate = raw.strip()
    if candidate in PLACEHOLDER_NULLS:
        return None

    for _ in range(2):
        try:
            decoded = json.loads(candidate)
        except Exception:
            break
        if isinstance(decoded, (dict, list)):
            return decoded
        if isinstance(decoded, str):
            candidate = decoded
            continue
    try:
        return json.loads(strip_outer_quotes(candidate) or candidate)
    except Exception:
        return None


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
        if len(cells) < len(headers):
            cells += [None] * (len(headers) - len(cells))
        data_rows.append(dict(zip(headers, cells)))
    return data_rows


def parse_existing_dataset(path: Path) -> Tuple[Sequence[str], List[str], int]:
    """Read the current new_dataset file, returning headers, preserved rows, and max id."""
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 2:
        raise ValueError(f"Dataset {path} is missing header rows.")

    header_lines = lines[:2]
    data_lines = [line for line in lines[2:] if line.strip()]
    headers = [header.strip() for header in header_lines[0].strip("|").split("|")]

    max_id = 0
    for line in data_lines:
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        row = dict(zip(headers, cells))
        row_id = parse_int(row.get("id"))
        if row_id is not None:
            max_id = max(max_id, row_id)
    return header_lines, data_lines, max_id


def parse_data_lines(header_line: str, data_lines: List[str]) -> List[Dict[str, Any]]:
    """Parse existing dataset lines into dictionaries keyed by headers."""
    headers = [header.strip() for header in header_line.strip("|").split("|")]
    parsed: List[Dict[str, Any]] = []
    for line in data_lines:
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < len(headers):
            cells += [None] * (len(headers) - len(cells))
        parsed.append(dict(zip(headers, cells)))
    return parsed


def load_id_map(path: Path) -> Dict[int, int]:
    """Load an old->new id mapping from an existing mapping file."""
    mapping_data = json.loads(path.read_text(encoding="utf-8"))
    return {entry["old_id"]: entry["new_id"] for entry in mapping_data.get("mapped", [])}


def build_face_analytics_map(path: Path) -> Dict[int, List[int]]:
    """Build a mapping of old stream_id -> list of new face analytics ids."""
    mapping_data = json.loads(path.read_text(encoding="utf-8"))
    stream_map: Dict[int, List[int]] = {}
    for entry in mapping_data.get("mapped", []):
        if entry.get("plugin_name") != "face":
            continue
        old_stream_id = entry.get("old_stream_id")
        new_id = entry.get("new_id")
        if old_stream_id is None or new_id is None:
            continue
        stream_map.setdefault(int(old_stream_id), []).append(int(new_id))
    for stream_id, ids in stream_map.items():
        stream_map[stream_id] = sorted(set(ids))
    return stream_map


def parse_int(value: Optional[str]) -> Optional[int]:
    """Parse an integer safely."""
    cleaned = clean_value(value)
    if cleaned is None:
        return None
    try:
        return int(cleaned.replace(",", ""))
    except Exception:
        return None


def parse_bool(value: Optional[str]) -> Optional[bool]:
    """Parse booleans from various string representations."""
    cleaned = clean_value(value)
    if cleaned is None:
        return None
    lowered = cleaned.lower()
    if lowered in {"true", "t", "1", "yes"}:
        return True
    if lowered in {"false", "f", "0", "no"}:
        return False
    return None


def map_streams_to_analytics(
    stream_ids: List[int], analytics_by_stream: Dict[int, List[int]]
) -> Tuple[List[int], List[int]]:
    """Map stream ids to analytics ids, tracking unmapped streams."""
    analytics_ids: List[int] = []
    unmapped: List[int] = []
    for stream_id in stream_ids:
        mapped_ids = analytics_by_stream.get(stream_id)
        if mapped_ids:
            analytics_ids.extend(mapped_ids)
        else:
            unmapped.append(stream_id)
    return sorted(set(analytics_ids)), sorted(set(unmapped))


def map_time_attendance(
    raw: Optional[Any], analytics_by_stream: Dict[int, List[int]]
) -> Tuple[Optional[Dict[str, Any]], List[int], List[int]]:
    """Remap time_attendance stream references to analytics ids."""
    if not isinstance(raw, dict):
        return (
            {"enabled": False, "entrance_analytics_ids": [], "exit_analytics_ids": []},
            [],
            [],
        )

    enabled = parse_bool(str(raw.get("enabled")))
    entrance_streams = raw.get("entrance_streams") or []
    exit_streams = raw.get("exit_streams") or []

    def to_int_list(values: Any) -> List[int]:
        result: List[int] = []
        for item in values or []:
            parsed = parse_int(str(item))
            if parsed is not None:
                result.append(parsed)
        return result

    entrance_ids = to_int_list(entrance_streams)
    exit_ids = to_int_list(exit_streams)

    entrance_analytics, unmapped_entrance = map_streams_to_analytics(entrance_ids, analytics_by_stream)
    exit_analytics, unmapped_exit = map_streams_to_analytics(exit_ids, analytics_by_stream)

    mapped = {
        "enabled": enabled if enabled is not None else False,
        "entrance_analytics_ids": entrance_analytics,
        "exit_analytics_ids": exit_analytics,
    }
    return mapped, unmapped_entrance, unmapped_exit


def format_cell(value: Any) -> str:
    """Format a value for dataset output."""
    if value is None:
        return "[NULL]"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def format_json_field(value: Optional[Dict[str, Any]]) -> str:
    """Format JSON payloads as compact JSON strings for datasets."""
    if value is None:
        return "[NULL]"
    return json.dumps(value, separators=(",", ":"))


def format_array(values: List[int]) -> str:
    """Format integer arrays for dataset output."""
    return json.dumps(values, separators=(",", ":"))


def sql_string(value: Optional[str]) -> str:
    """Escape a string for SQL output."""
    if value is None:
        return "NULL"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def sql_json(value: Optional[Dict[str, Any]]) -> str:
    """Format JSON payloads as SQL string literals."""
    if value is None:
        return "NULL"
    return sql_string(json.dumps(value, separators=(",", ":")))


def sql_array(values: List[int]) -> str:
    """Format integer arrays as SQL string literals."""
    return sql_string(json.dumps(values, separators=(",", ":")))


def sql_bool(value: Optional[bool]) -> str:
    """Format booleans for SQL output."""
    if value is None:
        return "NULL"
    return "true" if value else "false"


def sql_numeric(value: Optional[int]) -> str:
    """Format numeric values for SQL output."""
    if value is None:
        return "NULL"
    return str(value)


def build_face_list_records(
    legacy_rows: List[Dict[str, Optional[str]]],
    next_id: int,
    client_map: Dict[int, int],
    analytics_by_stream: Dict[int, List[int]],
    user_map: Dict[int, int],
) -> Tuple[List[FaceListRecord], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Normalize legacy rows into FaceListRecord instances."""

    records: List[FaceListRecord] = []
    mapped_entries: List[Dict[str, Any]] = []
    unmapped_old: List[Dict[str, Any]] = []

    for row in sorted(legacy_rows, key=lambda r: parse_int(r.get("id")) or 0):
        old_id = parse_int(row.get("id")) or 0
        status = parse_int(row.get("status")) or 0
        if status == -1:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "name": normalize_text(row.get("name")) or row.get("name"),
                    "old_client_id": parse_int(row.get("client_id")),
                    "reason": "status = -1 (excluded)",
                }
            )
            continue

        old_client_id = parse_int(row.get("client_id")) or 0
        new_client_id = client_map.get(old_client_id)
        if new_client_id is None:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "name": normalize_text(row.get("name")) or row.get("name"),
                    "old_client_id": old_client_id,
                    "reason": "client_id unmapped",
                }
            )
            continue

        streams_raw = parse_json_field(row.get("streams")) or []
        stream_ids = [parse_int(str(s)) for s in streams_raw]
        stream_ids = [sid for sid in stream_ids if sid is not None]
        analytics_ids, unmapped_streams = map_streams_to_analytics(stream_ids, analytics_by_stream)

        time_attendance_raw = parse_json_field(row.get("time_attendance"))
        time_attendance, unmapped_entrance, unmapped_exit = map_time_attendance(
            time_attendance_raw, analytics_by_stream
        )
        analytics_ids = sorted(set(analytics_ids + time_attendance.get("entrance_analytics_ids", []) + time_attendance.get("exit_analytics_ids", [])))
        all_unmapped_streams = sorted(set(unmapped_streams + unmapped_entrance + unmapped_exit))

        events_holder = parse_json_field(row.get("events_holder"))
        if events_holder is None:
            events_holder = {"events": [], "notify_enabled": False}

        list_permissions = parse_json_field(row.get("list_permissions")) or {}
        creator_id = list_permissions.get("creator_id")
        new_creator_id = user_map.get(int(creator_id)) if creator_id is not None else None
        if new_creator_id is not None:
            list_permissions["creator_id"] = new_creator_id

        comment = normalize_text(strip_outer_quotes(row.get("comment")))
        name = normalize_text(strip_outer_quotes(row.get("name"))) or ""
        color = (clean_value(row.get("color")) or "#FFFFFF").upper()

        min_conf = parse_int(row.get("min_confidence"))
        record = FaceListRecord(
            id=next_id,
            old_id=old_id,
            name=name,
            comment=comment,
            min_confidence=min_conf if min_conf is not None else 0,
            send_internal_notifications=parse_bool(row.get("send_internal_notifications")) is True,
            events_holder=events_holder,
            status=status,
            created_at=clean_value(row.get("created_at")),
            client_id=new_client_id,
            old_client_id=old_client_id,
            color=color,
            time_attendance=time_attendance,
            list_permissions=list_permissions,
            analytics_ids=analytics_ids,
            show_popup_for_internal_notifications=False,
            unmapped_stream_ids=all_unmapped_streams,
        )
        records.append(record)
        mapped_entries.append(
            {
                "old_id": old_id,
                "new_id": record.id,
                "name": name,
                "old_client_id": old_client_id,
                "new_client_id": new_client_id,
                "analytics_ids": analytics_ids,
                "unmapped_stream_ids": all_unmapped_streams,
                "status": status,
            }
        )
        next_id += 1

    return records, mapped_entries, unmapped_old


def write_face_lists_dataset(
    header_lines: Sequence[str],
    existing_lines: List[str],
    records: List[FaceListRecord],
    path: Path,
) -> None:
    """Write face lists dataset with preserved and migrated rows."""
    lines = list(header_lines) + list(existing_lines)
    for record in records:
        lines.append(
            "|".join(
                [
                    f"|{format_cell(record.id)}",
                    format_cell(record.name),
                    format_cell(record.comment),
                    format_cell(record.min_confidence),
                    format_cell(record.send_internal_notifications),
                    format_json_field(record.events_holder),
                    format_cell(record.status),
                    format_cell(record.created_at),
                    format_cell(record.client_id),
                    format_cell(record.color),
                    format_json_field(record.time_attendance),
                    format_json_field(record.list_permissions),
                    format_array(record.analytics_ids),
                    f"{format_cell(record.show_popup_for_internal_notifications)}|",
                ]
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_face_lists_sql(records: List[FaceListRecord], path: Path) -> None:
    """Write batched SQL insert for face lists."""
    if not records:
        path.write_text("", encoding="utf-8")
        return

    values = []
    for record in records:
        values.append(
            "  ("
            + ", ".join(
                [
                    str(record.id),
                    sql_string(record.name),
                    sql_string(record.comment),
                    sql_numeric(record.min_confidence),
                    sql_bool(record.send_internal_notifications),
                    sql_json(record.events_holder),
                    str(record.status),
                    sql_string(record.created_at),
                    str(record.client_id),
                    sql_string(record.color),
                    sql_json(record.time_attendance),
                    sql_json(record.list_permissions),
                    sql_array(record.analytics_ids),
                    sql_bool(record.show_popup_for_internal_notifications),
                ]
            )
            + ")"
        )
    sql = (
        "INSERT INTO videoanalytics.face_lists "
        "(id, \"name\", \"comment\", min_confidence, send_internal_notifications, events_holder, status, created_at, client_id, color, time_attendance, list_permissions, analytics_ids, show_popup_for_internal_notifications)\n"
        "VALUES\n"
        + ",\n".join(values)
        + ";\n"
    )
    path.write_text(sql, encoding="utf-8")


def write_mapping_file(
    path: Path,
    match_keys: List[str],
    mapped: List[Dict[str, Any]],
    unmapped_old: List[Dict[str, Any]],
    unmapped_new: List[Dict[str, Any]],
) -> None:
    """Persist a mapping file with required structure."""
    mapping = {
        "match_keys": match_keys,
        "mapped": mapped,
        "unmapped_old": unmapped_old,
        "unmapped_new": unmapped_new,
    }
    path.write_text(json.dumps(mapping, indent=2), encoding="utf-8")


def main() -> None:
    legacy_rows = parse_pipe_table(OLD_FACE_LISTS_PATH)
    header_lines, existing_lines, _ = parse_existing_dataset(NEW_FACE_LISTS_PATH)
    existing_rows = parse_data_lines(header_lines[0], existing_lines)

    preserved_lines: List[str] = []
    preserved_rows: List[Dict[str, Any]] = []
    max_preserved_id = 0
    for line, row in zip(existing_lines, existing_rows):
        row_id = parse_int(row.get("id"))
        if row_id is not None and (PRESERVE_FACE_LIST_IDS is None or row_id in PRESERVE_FACE_LIST_IDS):
            preserved_lines.append(line)
            preserved_rows.append(row)
            max_preserved_id = max(max_preserved_id, row_id)

    client_map = load_id_map(CLIENT_MAP_PATH)
    user_map = load_id_map(USER_MAP_PATH)
    analytics_by_stream = build_face_analytics_map(ANALYTICS_MAP_PATH)

    records, mapped_entries, unmapped_old = build_face_list_records(
        legacy_rows, max_preserved_id + 1, client_map, analytics_by_stream, user_map
    )

    write_face_lists_dataset(header_lines, preserved_lines, records, NEW_FACE_LISTS_PATH)
    write_face_lists_sql(records, SQL_FACE_LISTS_PATH)
    write_mapping_file(
        MAP_FACE_LISTS_PATH,
        [
            "name to name",
            "client_id to client_id",
            "status to status",
            "streams/time_attendance streams to analytics_ids via analytics mapping",
            "creator_id in list_permissions to users mapping",
        ],
        mapped_entries,
        unmapped_old,
        [
            {
                "new_id": parse_int(row.get("id")),
                "name": row.get("name"),
                "reason": "pre-existing new_dataset row",
            }
            for row in preserved_rows
        ],
    )


if __name__ == "__main__":
    main()

