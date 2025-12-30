#!/usr/bin/env python3
"""ALPR lists and list items migration helper.

Reads legacy ALPR lists and list items, normalizes text, remaps references
using existing mappings, and regenerates:
  - new_dataset/alpr_lists_202512301039.txt
  - new_dataset/alpr_list_items_202512301039.txt
  - sql/alpr_lists_inserts.sql
  - sql/alpr_list_items_inserts.sql
  - maps/alpr_lists.json
  - maps/alpr_list_items.json
"""
from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


OLD_LISTS_PATH = Path("old_dataset/_alpr_lists__202512301049.txt")
OLD_ITEMS_PATH = Path("old_dataset/_alpr_list_items__202512301049.txt")
NEW_LISTS_PATH = Path("new_dataset/alpr_lists_202512301039.txt")
NEW_ITEMS_PATH = Path("new_dataset/alpr_list_items_202512301039.txt")
CLIENT_MAP_PATH = Path("maps/clients.json")
USER_MAP_PATH = Path("maps/users.json")
ANALYTICS_MAP_PATH = Path("maps/analytics.json")
SQL_LISTS_PATH = Path("sql/alpr_lists_inserts.sql")
SQL_ITEMS_PATH = Path("sql/alpr_list_items_inserts.sql")
MAP_LISTS_PATH = Path("maps/alpr_lists.json")
MAP_ITEMS_PATH = Path("maps/alpr_list_items.json")
PRESERVE_LIST_IDS = {1}
PRESERVE_ITEM_IDS: set[int] = set()


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
class AlprListRecord:
    """Normalized ALPR list with remapped references."""

    id: int
    old_id: int
    name: str
    comment: Optional[str]
    analytics_ids: List[int]
    send_internal_notifications: bool
    events_holder: Optional[Dict[str, Any]]
    status: int
    created_at: Optional[str]
    list_permissions: Dict[str, Any]
    enabled: Optional[bool]
    color: str
    client_id: int
    old_client_id: int
    show_popup_for_internal_notifications: bool
    unmapped_stream_ids: List[int]


@dataclass
class AlprListItemRecord:
    """Normalized ALPR list item with remapped references."""

    id: int
    old_id: int
    number: str
    comment: Optional[str]
    status: Optional[int]
    created_at: Optional[str]
    created_by: Optional[int]
    old_created_by: Optional[int]
    closed_at: Optional[str]
    list_id: int
    old_list_id: int
    client_id: int
    old_client_id: int


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
    if raw is None or raw.strip() in PLACEHOLDER_NULLS:
        return None
    candidate = raw.strip()

    # Attempt to decode nested JSON strings until we reach an object/list.
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
        try:
            row_id = int((row.get("id") or "0").replace(",", ""))
            max_id = max(max_id, row_id)
        except Exception:
            continue
    return header_lines, data_lines, max_id


def load_id_map(path: Path) -> Dict[int, int]:
    """Load an old->new id mapping from an existing mapping file."""
    mapping_data = json.loads(path.read_text(encoding="utf-8"))
    return {entry["old_id"]: entry["new_id"] for entry in mapping_data.get("mapped", [])}


def build_alpr_analytics_map(path: Path) -> Dict[int, List[int]]:
    """Build a mapping of old stream_id -> list of new alpr analytics ids."""
    mapping_data = json.loads(path.read_text(encoding="utf-8"))
    stream_map: Dict[int, List[int]] = {}
    for entry in mapping_data.get("mapped", []):
        if entry.get("plugin_name") != "alpr":
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


def format_cell(value: Optional[Any]) -> str:
    """Format a value for dataset output."""
    if value is None:
        return "[NULL]"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def format_json_field(payload: Optional[Dict[str, Any]]) -> str:
    """Format JSON payloads for dataset output."""
    if payload is None:
        return "[NULL]"
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def format_array(values: List[int]) -> str:
    """Format integer arrays consistently."""
    return json.dumps(values, ensure_ascii=True, separators=(",", ":"))


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


def sql_array(values: List[int]) -> str:
    """Format integer arrays as SQL string literals."""
    return sql_string(json.dumps(values, ensure_ascii=True))


def sql_bool(value: Optional[bool]) -> str:
    """Format booleans for SQL output."""
    if value is None:
        return "NULL"
    return "true" if value else "false"


def sql_numeric(value: Optional[Any]) -> str:
    """Format numeric values for SQL output."""
    if value is None:
        return "NULL"
    return str(value)


def build_list_records(
    legacy_rows: List[Dict[str, Optional[str]]],
    next_id: int,
    client_map: Dict[int, int],
    analytics_by_stream: Dict[int, List[int]],
    user_map: Dict[int, int],
) -> Tuple[List[AlprListRecord], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Build normalized list records and mapping/unmapped sets."""
    records: List[AlprListRecord] = []
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
                    "reason": "status -1",
                }
            )
            continue

        old_client_id = parse_int(row.get("client_id"))
        new_client_id = client_map.get(old_client_id or 0)
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
        stream_ids = [int(s) for s in streams_raw if parse_int(str(s)) is not None]
        analytics_ids: List[int] = []
        unmapped_streams: List[int] = []
        for stream_id in stream_ids:
            mapped_ids = analytics_by_stream.get(stream_id)
            if mapped_ids:
                analytics_ids.extend(mapped_ids)
            else:
                unmapped_streams.append(stream_id)
        analytics_ids = sorted(set(analytics_ids))

        events_holder = parse_json_field(row.get("events_holder"))
        list_permissions = parse_json_field(row.get("list_permissions")) or {}
        creator_id = list_permissions.get("creator_id")
        new_creator_id = user_map.get(int(creator_id)) if creator_id is not None else None
        if new_creator_id is not None:
            list_permissions["creator_id"] = new_creator_id

        comment = normalize_text(strip_outer_quotes(row.get("comment")))
        name = normalize_text(strip_outer_quotes(row.get("name"))) or ""
        color = clean_value(row.get("color")) or "#FFFFFF"

        record = AlprListRecord(
            id=next_id,
            old_id=old_id,
            name=name,
            comment=comment,
            analytics_ids=analytics_ids,
            send_internal_notifications=parse_bool(row.get("send_internal_notifications")) is True,
            events_holder=events_holder,
            status=status,
            created_at=clean_value(row.get("created_at")),
            list_permissions=list_permissions,
            enabled=parse_bool(row.get("enabled")),
            color=color,
            client_id=new_client_id,
            old_client_id=old_client_id or 0,
            show_popup_for_internal_notifications=False,
            unmapped_stream_ids=unmapped_streams,
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
                "unmapped_stream_ids": unmapped_streams,
                "status": status,
            }
        )
        next_id += 1

    return records, mapped_entries, unmapped_old


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


def build_list_item_records(
    legacy_rows: List[Dict[str, Optional[str]]],
    next_id: int,
    list_id_map: Dict[int, int],
    client_map: Dict[int, int],
    user_map: Dict[int, int],
) -> Tuple[List[AlprListItemRecord], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Build normalized list item records and mapping/unmapped sets."""
    records: List[AlprListItemRecord] = []
    mapped_entries: List[Dict[str, Any]] = []
    unmapped_old: List[Dict[str, Any]] = []

    for row in sorted(legacy_rows, key=lambda r: parse_int(r.get("id")) or 0):
        old_id = parse_int(row.get("id")) or 0
        status = parse_int(row.get("status"))
        if status == -1:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "number": clean_value(row.get("number")),
                    "old_list_id": parse_int(row.get("list_id")),
                    "reason": "status -1",
                }
            )
            continue

        old_list_id = parse_int(row.get("list_id"))
        new_list_id = list_id_map.get(old_list_id or -1)
        if new_list_id is None:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "number": clean_value(row.get("number")),
                    "old_list_id": old_list_id,
                    "reason": "list not migrated",
                }
            )
            continue

        old_client_id = parse_int(row.get("client_id")) or 0
        new_client_id = client_map.get(old_client_id)
        if new_client_id is None:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "number": clean_value(row.get("number")),
                    "old_list_id": old_list_id,
                    "reason": "client_id unmapped",
                }
            )
            continue

        old_creator_id = parse_int(row.get("created_by"))
        new_creator_id = user_map.get(old_creator_id) if old_creator_id is not None else None

        normalized_number = normalize_text(strip_outer_quotes(row.get("number"))) or ""
        normalized_comment = normalize_text(strip_outer_quotes(row.get("comment")))

        record = AlprListItemRecord(
            id=next_id,
            old_id=old_id,
            number=normalized_number,
            comment=normalized_comment,
            status=status,
            created_at=clean_value(row.get("created_at")),
            created_by=new_creator_id,
            old_created_by=old_creator_id,
            closed_at=clean_value(row.get("closed_at")),
            list_id=new_list_id,
            old_list_id=old_list_id or 0,
            client_id=new_client_id,
            old_client_id=old_client_id,
        )
        records.append(record)
        mapped_entries.append(
            {
                "old_id": old_id,
                "new_id": record.id,
                "number": normalized_number,
                "old_list_id": old_list_id,
                "new_list_id": new_list_id,
                "old_client_id": old_client_id,
                "new_client_id": new_client_id,
                "old_created_by": old_creator_id,
                "new_created_by": new_creator_id,
                "status": status,
            }
        )
        next_id += 1

    return records, mapped_entries, unmapped_old


def write_lists_dataset(
    header_lines: Sequence[str],
    existing_lines: List[str],
    records: List[AlprListRecord],
    path: Path,
) -> None:
    """Write merged ALPR lists dataset."""
    lines = list(header_lines) + list(existing_lines)
    for record in records:
        lines.append(
            "|".join(
                [
                    f"|{format_cell(record.id)}",
                    format_cell(record.name),
                    format_cell(record.comment),
                    format_array(record.analytics_ids),
                    format_cell(record.send_internal_notifications),
                    format_json_field(record.events_holder),
                    format_cell(record.status),
                    format_cell(record.created_at),
                    format_json_field(record.list_permissions),
                    format_cell(record.enabled),
                    format_cell(record.color),
                    format_cell(record.client_id),
                    f"{format_cell(record.show_popup_for_internal_notifications)}|",
                ]
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_items_dataset(
    header_lines: Sequence[str],
    existing_lines: List[str],
    records: List[AlprListItemRecord],
    path: Path,
) -> None:
    """Write ALPR list items dataset."""
    lines = list(header_lines) + list(existing_lines)
    for record in records:
        lines.append(
            "|".join(
                [
                    f"|{format_cell(record.id)}",
                    format_cell(record.number),
                    format_cell(record.comment),
                    format_cell(record.status),
                    format_cell(record.created_at),
                    format_cell(record.created_by),
                    format_cell(record.closed_at),
                    format_cell(record.list_id),
                    f"{format_cell(record.client_id)}|",
                ]
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_lists_sql(records: List[AlprListRecord], path: Path) -> None:
    """Write batched SQL insert for ALPR lists."""
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
                    sql_array(record.analytics_ids),
                    sql_bool(record.send_internal_notifications),
                    sql_json(record.events_holder),
                    str(record.status),
                    sql_string(record.created_at),
                    sql_json(record.list_permissions),
                    sql_bool(record.enabled),
                    sql_string(record.color),
                    str(record.client_id),
                    sql_bool(record.show_popup_for_internal_notifications),
                ]
            )
            + ")"
        )
    sql = (
        "INSERT INTO videoanalytics.alpr_lists "
        "(id, \"name\", \"comment\", analytics_ids, send_internal_notifications, events_holder, status, created_at, list_permissions, enabled, color, client_id, show_popup_for_internal_notifications)\n"
        "VALUES\n"
        + ",\n".join(values)
        + ";\n"
    )
    path.write_text(sql, encoding="utf-8")


def write_items_sql(records: List[AlprListItemRecord], path: Path) -> None:
    """Write batched SQL insert for ALPR list items."""
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
                    sql_string(record.number),
                    sql_string(record.comment),
                    sql_numeric(record.status),
                    sql_string(record.created_at),
                    sql_numeric(record.created_by),
                    sql_string(record.closed_at),
                    str(record.list_id),
                    str(record.client_id),
                ]
            )
            + ")"
        )
    sql = (
        "INSERT INTO videoanalytics.alpr_list_items "
        "(id, \"number\", \"comment\", status, created_at, created_by, closed_at, list_id, client_id)\n"
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
    legacy_lists = parse_pipe_table(OLD_LISTS_PATH)
    legacy_items = parse_pipe_table(OLD_ITEMS_PATH)
    header_lists, existing_list_lines, _ = parse_existing_dataset(NEW_LISTS_PATH)
    header_items, existing_item_lines, _ = parse_existing_dataset(NEW_ITEMS_PATH)
    existing_list_rows = parse_data_lines(header_lists[0], existing_list_lines)
    existing_item_rows = parse_data_lines(header_items[0], existing_item_lines)

    preserved_list_lines: List[str] = []
    preserved_list_rows: List[Dict[str, Any]] = []
    max_preserved_list_id = 0
    for line, row in zip(existing_list_lines, existing_list_rows):
        row_id = parse_int(row.get("id"))
        if row_id is not None and (PRESERVE_LIST_IDS is None or row_id in PRESERVE_LIST_IDS):
            preserved_list_lines.append(line)
            preserved_list_rows.append(row)
            max_preserved_list_id = max(max_preserved_list_id, row_id)

    preserved_item_lines: List[str] = []
    preserved_item_rows: List[Dict[str, Any]] = []
    max_preserved_item_id = 0
    for line, row in zip(existing_item_lines, existing_item_rows):
        row_id = parse_int(row.get("id"))
        if row_id is not None and (PRESERVE_ITEM_IDS is None or row_id in PRESERVE_ITEM_IDS):
            preserved_item_lines.append(line)
            preserved_item_rows.append(row)
            max_preserved_item_id = max(max_preserved_item_id, row_id)

    client_map = load_id_map(CLIENT_MAP_PATH)
    user_map = load_id_map(USER_MAP_PATH)
    analytics_by_stream = build_alpr_analytics_map(ANALYTICS_MAP_PATH)

    list_records, list_mapped, list_unmapped_old = build_list_records(
        legacy_lists, max_preserved_list_id + 1, client_map, analytics_by_stream, user_map
    )
    list_id_map = {entry["old_id"]: entry["new_id"] for entry in list_mapped}

    item_records, item_mapped, item_unmapped_old = build_list_item_records(
        legacy_items, max_preserved_item_id + 1, list_id_map, client_map, user_map
    )

    write_lists_dataset(header_lists, preserved_list_lines, list_records, NEW_LISTS_PATH)
    write_items_dataset(header_items, preserved_item_lines, item_records, NEW_ITEMS_PATH)
    write_lists_sql(list_records, SQL_LISTS_PATH)
    write_items_sql(item_records, SQL_ITEMS_PATH)

    write_mapping_file(
        MAP_LISTS_PATH,
        [
            "name to name",
            "client_id to client_id",
            "status to status",
            "streams to analytics_ids via analytics mapping",
            "created_at to created_at",
        ],
        list_mapped,
        list_unmapped_old,
        [
            {
                "new_id": parse_int(row.get("id")),
                "name": row.get("name"),
                "reason": "pre-existing new_dataset row",
            }
            for row in preserved_list_rows
        ],
    )

    write_mapping_file(
        MAP_ITEMS_PATH,
        [
            "number to number",
            "list_id to list_id (remapped via alpr_lists)",
            "client_id to client_id",
            "created_by to created_by via users mapping",
            "status to status",
        ],
        item_mapped,
        item_unmapped_old,
        [
            {
                "new_id": parse_int(row.get("id")),
                "number": row.get("number"),
                "reason": "pre-existing new_dataset row",
            }
            for row in preserved_item_rows
        ],
    )


if __name__ == "__main__":
    main()
