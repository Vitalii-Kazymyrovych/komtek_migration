#!/usr/bin/env python3
"""Face lists migration helper.

Reads legacy face lists, items, and images, normalizes text, remaps
dependencies using existing mappings, regenerates datasets/SQL artifacts,
updates mapping files, and rehomes image assets into per-list folders with
human-readable filenames.
"""
from __future__ import annotations

import json
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


OLD_FACE_LISTS_PATH = Path("old_dataset/_face_lists__202512301049.txt")
OLD_FACE_LIST_ITEMS_PATH = Path("old_dataset/_face_list_items__202512301049.txt")
OLD_FACE_LIST_ITEMS_IMAGES_PATH = Path("old_dataset/_face_list_items_images__202512301049.txt")

NEW_FACE_LISTS_PATH = Path("new_dataset/face_lists_202512301039.txt")
NEW_FACE_LIST_ITEMS_PATH = Path("new_dataset/face_list_items_202512301039.txt")
NEW_FACE_LIST_ITEMS_IMAGES_PATH = Path("new_dataset/face_list_items_images_202512301039.txt")

CLIENT_MAP_PATH = Path("maps/clients.json")
USER_MAP_PATH = Path("maps/users.json")
ANALYTICS_MAP_PATH = Path("maps/analytics.json")

SQL_FACE_LISTS_PATH = Path("sql/face_lists_inserts.sql")
SQL_FACE_LIST_ITEMS_PATH = Path("sql/face_list_items_inserts.sql")
SQL_FACE_LIST_ITEMS_IMAGES_PATH = Path("sql/face_list_items_images_inserts.sql")

MAP_FACE_LISTS_PATH = Path("maps/face_lists.json")
MAP_FACE_LIST_ITEMS_PATH = Path("maps/face_list_items.json")
MAP_FACE_LIST_ITEMS_IMAGES_PATH = Path("maps/face_list_items_images.json")

FACE_LISTS_ROOT = Path("face_lists")
PART_DIRECTORIES = [FACE_LISTS_ROOT / "part1", FACE_LISTS_ROOT / "part2"]

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

NULL_VALUES = {"", "-", "NULL", "null", "[NULL]"}


# ---------------------------------------------------------------------------
# Parsing helpers


def normalize_text(value: Optional[str]) -> Optional[str]:
    """Normalize text to ASCII, stripping whitespace and diacritics."""

    if value is None:
        return None

    trimmed = value.strip()
    if trimmed in NULL_VALUES:
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
    if trimmed in NULL_VALUES:
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

    if isinstance(value, int):
        return value
    cleaned = clean_value(value)
    if cleaned is None:
        return None
    numeric = cleaned.replace(",", "")
    try:
        return int(numeric)
    except ValueError:
        return None


def parse_json_field(value: Optional[str]) -> Optional[Any]:
    """Parse a JSON payload that may be double-quoted and escape-encoded."""

    cleaned = clean_value(value)
    if cleaned is None:
        return None

    unquoted = strip_outer_quotes(cleaned)
    if unquoted is None:
        return None
    decoded = unquoted.encode("utf-8").decode("unicode_escape")
    try:
        return json.loads(decoded)
    except json.JSONDecodeError:
        try:
            return json.loads(unquoted, strict=False)
        except json.JSONDecodeError:
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
        data_rows.append(dict(zip(headers, cells)))
    return data_rows


def parse_existing_dataset(
    path: Path,
) -> Tuple[Sequence[str], List[str], List[Dict[str, Any]], int]:
    """Read the current new_dataset file, preserving existing rows.

    Returns (header_lines, existing_line_strings, parsed_existing_rows, max_existing_id).
    """

    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 2:
        raise ValueError(f"Existing dataset is missing header rows: {path}")

    header_lines = lines[:2]
    data_lines = [line for line in lines[2:] if line.strip()]

    headers = [header.strip() for header in header_lines[0].strip("|").split("|")]
    parsed_rows: List[Dict[str, Any]] = []
    max_id = 0
    for line in data_lines:
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        row = dict(zip(headers, cells))
        try:
            row_id = int(row["id"].replace(",", ""))
        except Exception:
            continue
        max_id = max(max_id, row_id)
        parsed_rows.append(row)

    return header_lines, data_lines, parsed_rows, max_id


def load_id_map(path: Path) -> Dict[int, int]:
    """Load an old->new id mapping from an existing mapping file."""

    mapping_data = json.loads(path.read_text(encoding="utf-8"))
    return {entry["old_id"]: entry["new_id"] for entry in mapping_data.get("mapped", [])}


def load_face_analytics_by_stream(path: Path) -> Dict[int, List[int]]:
    """Build a mapping from legacy stream ids to face analytics ids."""

    data = json.loads(path.read_text(encoding="utf-8"))
    stream_to_analytics: Dict[int, List[int]] = defaultdict(list)
    for entry in data.get("mapped", []):
        if entry.get("plugin_name") != "face":
            continue
        old_stream_id = entry.get("old_stream_id")
        new_id = entry.get("new_id")
        if old_stream_id is None or new_id is None:
            continue
        if new_id not in stream_to_analytics[old_stream_id]:
            stream_to_analytics[old_stream_id].append(new_id)
    for analytics_ids in stream_to_analytics.values():
        analytics_ids.sort()
    return stream_to_analytics


def parse_int_list(raw_value: Optional[str]) -> List[int]:
    """Parse a JSON-like list of integers."""

    cleaned = clean_value(raw_value)
    if cleaned is None:
        return []
    try:
        parsed = json.loads(strip_outer_quotes(cleaned) or "[]")
        return [int(str(item).replace(",", "")) for item in parsed if str(item).strip()]
    except (json.JSONDecodeError, TypeError, ValueError):
        return []


def slugify(value: Optional[str], fallback: str) -> str:
    """Convert a string into a filesystem-safe slug."""

    normalized = normalize_text(value) or fallback
    slug = re.sub(r"[^A-Za-z0-9]+", "_", normalized)
    slug = slug.strip("_") or fallback
    return slug.lower()


def ms_to_timestamp(ms_value: Optional[Any]) -> Optional[str]:
    """Convert a millisecond epoch value to a timestamp string."""

    if ms_value is None:
        return None
    try:
        ms_int = int(str(ms_value).replace(",", ""))
    except ValueError:
        return None
    if ms_int <= 0:
        return None
    dt = datetime.fromtimestamp(ms_int / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def format_json_field(payload: Optional[Any]) -> str:
    """Serialize JSON payloads for dataset/SQL output."""

    if payload is None:
        return "[NULL]"
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def format_cell(value: Optional[Any]) -> str:
    """Serialize a dataset cell to pipe-table format."""

    if value is None:
        return "[NULL]"
    return str(value)


def sql_string(value: Optional[str]) -> str:
    """Escape a string for SQL output."""

    if value is None:
        return "NULL"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def sql_json(payload: Optional[Any]) -> str:
    """Format JSON payloads as SQL string literals."""

    if payload is None:
        return "NULL"
    return sql_string(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))


def sql_numeric(value: Optional[Any]) -> str:
    """Format numeric values for SQL output."""

    if value is None:
        return "NULL"
    return str(value)


def sql_bool(value: Optional[Any]) -> str:
    """Format boolean values for SQL output."""

    if value is None:
        return "NULL"
    if isinstance(value, str):
        normalized = value.strip().lower()
        return "true" if normalized == "true" else "false"
    return "true" if bool(value) else "false"


# ---------------------------------------------------------------------------
# Dataclasses


@dataclass
class FaceListRecord:
    id: int
    old_id: int
    name: str
    comment: Optional[str]
    min_confidence: int
    send_internal_notifications: bool
    events_holder: Optional[Any]
    status: int
    created_at: Optional[str]
    client_id: int
    old_client_id: int
    color: str
    time_attendance: Optional[Any]
    list_permissions: Optional[Any]
    analytics_ids: List[int]
    show_popup_for_internal_notifications: bool


@dataclass
class FaceListItemRecord:
    id: int
    old_id: int
    name: str
    comment: Optional[str]
    status: int
    created_at: Optional[str]
    created_by: int
    old_created_by: Optional[int]
    closed_at: Optional[str]
    list_id: int
    old_list_id: int
    expiration_settings: Optional[Any]
    client_id: int
    old_client_id: int
    expiration_settings_enabled: bool
    expiration_settings_action: str
    expiration_settings_list_id: Optional[int]
    expiration_settings_date: Optional[str]
    expiration_settings_events_holder: Optional[Any]


@dataclass
class FaceListItemImageRecord:
    id: int
    old_id: int
    list_item_id: int
    old_list_item_id: int
    path: str
    encoding: Optional[str]
    points: Optional[str]


# ---------------------------------------------------------------------------
# Record builders


def build_face_lists(
    rows: List[Dict[str, Optional[str]]],
    client_map: Dict[int, int],
    face_analytics_map: Dict[int, List[int]],
    existing_max_id: int,
    user_map: Dict[int, int],
) -> Tuple[List[FaceListRecord], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Normalize and remap legacy face lists."""

    records: List[FaceListRecord] = []
    unmapped_old: List[Dict[str, Any]] = []
    next_id = existing_max_id + 1

    for row in rows:
        old_id = to_int(row.get("id"))
        if old_id is None:
            continue

        status = to_int(row.get("status")) or 0
        if status == -1:
            unmapped_old.append({"old_id": old_id, "reason": "status = -1"})
            continue

        old_client_id = to_int(row.get("client_id"))
        if old_client_id is None or old_client_id not in client_map:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "name": normalize_text(row.get("name")),
                    "reason": "missing client mapping",
                    "old_client_id": old_client_id,
                }
            )
            continue

        name = normalize_text(row.get("name")) or f"Face List {old_id}"
        comment = normalize_text(strip_outer_quotes(row.get("comment")))
        min_confidence = to_int(row.get("min_confidence")) or 80
        send_notifications = str(clean_value(row.get("send_internal_notifications")) or "false").lower()
        send_internal_notifications = send_notifications == "true"
        events_holder = parse_json_field(row.get("events_holder")) or {"events": [], "notify_enabled": False}
        created_at = clean_value(row.get("created_at"))
        color = normalize_text(row.get("color")) or "#FFFFFF"

        streams = parse_int_list(row.get("streams"))
        analytics_ids: List[int] = []
        for stream_id in streams:
            analytics_ids.extend(face_analytics_map.get(stream_id, []))
        analytics_ids = sorted(set(analytics_ids))

        time_attendance_payload = parse_json_field(row.get("time_attendance"))
        time_attendance: Optional[Dict[str, Any]] = None
        if isinstance(time_attendance_payload, dict):
            entrance_streams = parse_int_list(json.dumps(time_attendance_payload.get("entrance_streams", [])))
            exit_streams = parse_int_list(json.dumps(time_attendance_payload.get("exit_streams", [])))
            entrance_analytics: List[int] = []
            exit_analytics: List[int] = []
            for stream_id in entrance_streams:
                entrance_analytics.extend(face_analytics_map.get(stream_id, []))
            for stream_id in exit_streams:
                exit_analytics.extend(face_analytics_map.get(stream_id, []))
            time_attendance = {
                "enabled": bool(time_attendance_payload.get("enabled", False)),
                "entrance_analytics_ids": sorted(set(entrance_analytics)),
                "exit_analytics_ids": sorted(set(exit_analytics)),
            }

        permissions = parse_json_field(row.get("list_permissions"))
        if isinstance(permissions, dict) and "creator_id" in permissions:
            creator_id = to_int(str(permissions.get("creator_id")))
            if creator_id is not None and creator_id in user_map:
                permissions["creator_id"] = user_map[creator_id]
            elif creator_id is not None:
                permissions["creator_id"] = 1
        if permissions is None:
            permissions = {
                "default_permissions": {},
                "role_permissions": {},
                "user_permissions": {},
            }

        show_popup = bool(events_holder.get("notify_enabled"))

        record = FaceListRecord(
            id=next_id,
            old_id=old_id,
            name=name,
            comment=comment,
            min_confidence=min_confidence,
            send_internal_notifications=send_internal_notifications,
            events_holder=events_holder,
            status=status,
            created_at=created_at,
            client_id=client_map[old_client_id],
            old_client_id=old_client_id,
            color=color,
            time_attendance=time_attendance,
            list_permissions=permissions,
            analytics_ids=analytics_ids,
            show_popup_for_internal_notifications=show_popup,
        )
        records.append(record)
        next_id += 1

    records.sort(key=lambda record: record.id)
    return records, unmapped_old, []


def build_face_list_items(
    rows: List[Dict[str, Optional[str]]],
    list_id_map: Dict[int, int],
    client_map: Dict[int, int],
    user_map: Dict[int, int],
    existing_max_id: int,
) -> Tuple[List[FaceListItemRecord], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Normalize and remap face list items."""

    records: List[FaceListItemRecord] = []
    unmapped_old: List[Dict[str, Any]] = []
    next_id = existing_max_id + 1

    for row in rows:
        old_id = to_int(row.get("id"))
        if old_id is None:
            continue

        status = to_int(row.get("status")) or 0
        if status == -1:
            unmapped_old.append({"old_id": old_id, "reason": "status = -1"})
            continue

        old_list_id = to_int(row.get("list_id"))
        if old_list_id is None or old_list_id not in list_id_map:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "reason": "missing list mapping",
                    "old_list_id": old_list_id,
                }
            )
            continue

        old_client_id = to_int(row.get("client_id"))
        if old_client_id is None or old_client_id not in client_map:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "reason": "missing client mapping",
                    "old_client_id": old_client_id,
                }
            )
            continue

        name = normalize_text(row.get("name")) or f"Face Item {old_id}"
        comment = normalize_text(strip_outer_quotes(row.get("comment")))
        created_at = clean_value(row.get("created_at"))
        closed_at = clean_value(row.get("closed_at"))
        old_created_by = to_int(row.get("created_by"))
        created_by = user_map.get(old_created_by, 1)

        expiration_raw = parse_json_field(row.get("expiration_settings")) or {}
        expiration_enabled = bool(expiration_raw.get("enabled", False))
        expiration_action = normalize_text(str(expiration_raw.get("action", "none"))) or "none"
        expiration_list_old = to_int(expiration_raw.get("list_id"))
        expiration_list_new = list_id_map.get(expiration_list_old) if expiration_list_old else None
        expiration_date = ms_to_timestamp(expiration_raw.get("expires_at"))
        expiration_events_holder = expiration_raw.get("events_holder") or {"events": [], "notify_enabled": False}
        expiration_settings_payload = {
            "action": expiration_action,
            "enabled": expiration_enabled,
            "events_holder": expiration_events_holder,
            "expires_at": expiration_raw.get("expires_at", 0) or 0,
        }
        if expiration_list_new is not None:
            expiration_settings_payload["list_id"] = expiration_list_new

        record = FaceListItemRecord(
            id=next_id,
            old_id=old_id,
            name=name,
            comment=comment,
            status=status,
            created_at=created_at,
            created_by=created_by,
            old_created_by=old_created_by,
            closed_at=closed_at,
            list_id=list_id_map[old_list_id],
            old_list_id=old_list_id,
            expiration_settings=expiration_settings_payload,
            client_id=client_map[old_client_id],
            old_client_id=old_client_id,
            expiration_settings_enabled=expiration_enabled,
            expiration_settings_action=expiration_action,
            expiration_settings_list_id=expiration_list_new,
            expiration_settings_date=expiration_date,
            expiration_settings_events_holder=expiration_events_holder,
        )
        records.append(record)
        next_id += 1

    records.sort(key=lambda record: record.id)
    return records, unmapped_old, []


def build_face_list_item_images(
    rows: List[Dict[str, Optional[str]]],
    list_item_id_map: Dict[int, int],
    item_to_list_map: Dict[int, int],
    file_index: Dict[str, Path],
    list_folder_map: Dict[int, Path],
    item_slug_map: Dict[int, str],
    existing_max_id: int,
) -> Tuple[List[FaceListItemImageRecord], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Normalize and remap face list item images, renaming files."""

    records: List[FaceListItemImageRecord] = []
    unmapped_old: List[Dict[str, Any]] = []
    next_id = existing_max_id + 1
    item_image_counters: Dict[int, int] = defaultdict(int)

    for row in rows:
        old_id = to_int(row.get("id"))
        if old_id is None:
            continue
        old_list_item_id = to_int(row.get("list_item_id"))
        if old_list_item_id is None or old_list_item_id not in list_item_id_map:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "reason": "missing list_item mapping",
                    "old_list_item_id": old_list_item_id,
                }
            )
            continue

        raw_path = strip_outer_quotes(row.get("path"))
        base_name = Path(raw_path or "").name
        source_path = file_index.get(base_name)
        if source_path is None:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "reason": "source image not found",
                    "old_path": raw_path,
                    "old_list_item_id": old_list_item_id,
                }
            )
            continue

        new_list_id = item_to_list_map.get(old_list_item_id)
        if new_list_id is None or new_list_id not in list_folder_map:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "reason": "list folder missing for item",
                    "old_list_item_id": old_list_item_id,
                    "new_list_id": new_list_id,
                }
            )
            continue

        item_image_counters[old_list_item_id] += 1
        counter = item_image_counters[old_list_item_id]
        slug = item_slug_map.get(old_list_item_id, f"item_{old_list_item_id}")
        extension = source_path.suffix or ".jpg"
        new_filename = f"{list_item_id_map[old_list_item_id]}_{slug}_{counter:02d}{extension}"
        list_folder = list_folder_map[new_list_id]
        destination_path = list_folder / new_filename
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.rename(destination_path)

        record = FaceListItemImageRecord(
            id=next_id,
            old_id=old_id,
            list_item_id=list_item_id_map[old_list_item_id],
            old_list_item_id=old_list_item_id,
            path=destination_path.as_posix(),
            encoding=clean_value(row.get("encoding")),
            points=clean_value(row.get("points")),
        )
        records.append(record)
        next_id += 1

    records.sort(key=lambda record: record.id)
    return records, unmapped_old, []


# ---------------------------------------------------------------------------
# Writers


def write_face_lists_dataset(
    header_lines: Sequence[str],
    existing_lines: List[str],
    records: List[FaceListRecord],
    path: Path,
) -> None:
    lines = list(header_lines) + list(existing_lines)
    for record in records:
        lines.append(
            "|".join(
                [
                    f"|{format_cell(record.id)}",
                    format_cell(record.name),
                    format_cell(record.comment),
                    format_cell(record.min_confidence),
                    format_cell(str(record.send_internal_notifications).lower()),
                    format_json_field(record.events_holder),
                    format_cell(record.status),
                    format_cell(record.created_at),
                    format_cell(record.client_id),
                    format_cell(record.color),
                    format_json_field(record.time_attendance),
                    format_json_field(record.list_permissions),
                    format_json_field(record.analytics_ids),
                    f"{format_cell(str(record.show_popup_for_internal_notifications).lower())}|",
                ]
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_face_list_items_dataset(
    header_lines: Sequence[str],
    existing_lines: List[str],
    records: List[FaceListItemRecord],
    path: Path,
) -> None:
    lines = list(header_lines) + list(existing_lines)
    for record in records:
        lines.append(
            "|".join(
                [
                    f"|{format_cell(record.id)}",
                    format_cell(record.name),
                    format_cell(record.comment),
                    format_cell(record.status),
                    format_cell(record.created_at),
                    format_cell(record.created_by),
                    format_cell(record.closed_at),
                    format_cell(record.list_id),
                    format_json_field(record.expiration_settings),
                    format_cell(record.client_id),
                    format_cell(str(record.expiration_settings_enabled).lower()),
                    format_cell(record.expiration_settings_action),
                    format_cell(record.expiration_settings_list_id),
                    format_cell(record.expiration_settings_date),
                    f"{format_json_field(record.expiration_settings_events_holder)}|",
                ]
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_face_list_item_images_dataset(
    header_lines: Sequence[str],
    existing_lines: List[str],
    records: List[FaceListItemImageRecord],
    path: Path,
) -> None:
    lines = list(header_lines) + list(existing_lines)
    for record in records:
        lines.append(
            "|".join(
                [
                    f"|{format_cell(record.id)}",
                    format_cell(record.list_item_id),
                    format_cell(record.path),
                    format_cell(record.encoding),
                    f"{format_cell(record.points)}|",
                ]
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_face_lists_sql(records: List[FaceListRecord], path: Path) -> None:
    value_lines = []
    for record in records:
        value_lines.append(
            "  ("
            + ", ".join(
                [
                    sql_numeric(record.id),
                    sql_string(record.name),
                    sql_string(record.comment),
                    sql_numeric(record.min_confidence),
                    sql_bool(record.send_internal_notifications),
                    sql_json(record.events_holder),
                    sql_numeric(record.status),
                    sql_string(record.created_at),
                    sql_numeric(record.client_id),
                    sql_string(record.color),
                    sql_json(record.time_attendance),
                    sql_json(record.list_permissions),
                    sql_json(record.analytics_ids),
                    sql_bool(record.show_popup_for_internal_notifications),
                ]
            )
            + ")"
        )

    sql = (
        'INSERT INTO videoanalytics.face_lists (id, "name", "comment", min_confidence, send_internal_notifications, events_holder, status, created_at, client_id, color, time_attendance, list_permissions, analytics_ids, show_popup_for_internal_notifications)\nVALUES\n'
        + ",\n".join(value_lines)
        + ";\n"
    )
    path.write_text(sql, encoding="utf-8")


def write_face_list_items_sql(records: List[FaceListItemRecord], path: Path) -> None:
    value_lines = []
    for record in records:
        value_lines.append(
            "  ("
            + ", ".join(
                [
                    sql_numeric(record.id),
                    sql_string(record.name),
                    sql_string(record.comment),
                    sql_numeric(record.status),
                    sql_string(record.created_at),
                    sql_numeric(record.created_by),
                    sql_string(record.closed_at),
                    sql_numeric(record.list_id),
                    sql_json(record.expiration_settings),
                    sql_numeric(record.client_id),
                    sql_bool(record.expiration_settings_enabled),
                    sql_string(record.expiration_settings_action),
                    sql_numeric(record.expiration_settings_list_id),
                    sql_string(record.expiration_settings_date),
                    sql_json(record.expiration_settings_events_holder),
                ]
            )
            + ")"
        )

    sql = (
        'INSERT INTO videoanalytics.face_list_items (id, "name", "comment", status, created_at, created_by, closed_at, list_id, expiration_settings, client_id, expiration_settings_enabled, expiration_settings_action, expiration_settings_list_id, expiration_settings_date, expiration_settings_events_holder)\nVALUES\n'
        + ",\n".join(value_lines)
        + ";\n"
    )
    path.write_text(sql, encoding="utf-8")


def write_face_list_items_images_sql(records: List[FaceListItemImageRecord], path: Path) -> None:
    value_lines = []
    for record in records:
        value_lines.append(
            "  ("
            + ", ".join(
                [
                    sql_numeric(record.id),
                    sql_numeric(record.list_item_id),
                    sql_string(record.path),
                    sql_string(record.encoding),
                    sql_string(record.points),
                ]
            )
            + ")"
        )

    sql = (
        'INSERT INTO videoanalytics.face_list_items_images (id, list_item_id, "path", encoding, points)\nVALUES\n'
        + ",\n".join(value_lines)
        + ";\n"
    )
    path.write_text(sql, encoding="utf-8")


def write_mapping(
    mapped: List[Dict[str, Any]],
    unmapped_old: List[Dict[str, Any]],
    unmapped_new: List[Dict[str, Any]],
    match_keys: List[str],
    path: Path,
) -> None:
    mapping = {
        "match_keys": match_keys,
        "mapped": mapped,
        "unmapped_old": unmapped_old,
        "unmapped_new": unmapped_new,
    }
    path.write_text(json.dumps(mapping, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# File indexing


def index_image_files(directories: Iterable[Path]) -> Dict[str, Path]:
    """Index available image files by basename across provided directories."""

    index: Dict[str, Path] = {}
    for directory in directories:
        if not directory.exists():
            continue
        for path in sorted(directory.glob("**/*")):
            if path.is_file():
                index[path.name] = path
    return index


# ---------------------------------------------------------------------------
# Orchestrator


def main() -> None:
    client_map = load_id_map(CLIENT_MAP_PATH)
    user_map = load_id_map(USER_MAP_PATH)
    face_analytics_map = load_face_analytics_by_stream(ANALYTICS_MAP_PATH)
    file_index = index_image_files(PART_DIRECTORIES)

    # Face lists
    list_rows = parse_pipe_table(OLD_FACE_LISTS_PATH)
    list_headers, list_existing_lines, list_existing_rows, list_max_id = parse_existing_dataset(
        NEW_FACE_LISTS_PATH
    )
    list_records, list_unmapped_old, list_unmapped_new = build_face_lists(
        list_rows, client_map, face_analytics_map, list_max_id, user_map
    )
    list_id_map = {record.old_id: record.id for record in list_records}
    list_folder_map = {
        record.id: FACE_LISTS_ROOT / f"{record.id}_{slugify(record.name, f'list_{record.id}')}"
        for record in list_records
    }

    write_face_lists_dataset(list_headers, list_existing_lines, list_records, NEW_FACE_LISTS_PATH)
    write_face_lists_sql(list_records, SQL_FACE_LISTS_PATH)

    write_mapping(
        [
            {
                "old_id": record.old_id,
                "new_id": record.id,
                "name": record.name,
                "old_client_id": record.old_client_id,
                "new_client_id": record.client_id,
                "analytics_ids": record.analytics_ids,
            }
            for record in list_records
        ],
        list_unmapped_old,
        [
            {"new_id": to_int(row.get("id")), "name": row.get("name")}
            for row in list_existing_rows
        ],
        [
            "existing new_dataset rows preserved",
            "legacy status != -1 lists remapped sequentially to id",
            "client_id remapped via clients.json to client_id",
            "streams converted to face analytics ids via analytics.json",
        ],
        MAP_FACE_LISTS_PATH,
    )

    # Face list items
    item_rows = parse_pipe_table(OLD_FACE_LIST_ITEMS_PATH)
    item_headers, item_existing_lines, item_existing_rows, item_max_id = parse_existing_dataset(
        NEW_FACE_LIST_ITEMS_PATH
    )
    item_records, item_unmapped_old, item_unmapped_new = build_face_list_items(
        item_rows, list_id_map, client_map, user_map, item_max_id
    )
    item_id_map = {record.old_id: record.id for record in item_records}
    item_slug_map = {record.old_id: slugify(record.name, f"item_{record.id}") for record in item_records}
    item_to_list_map = {record.old_id: record.list_id for record in item_records}

    write_face_list_items_dataset(item_headers, item_existing_lines, item_records, NEW_FACE_LIST_ITEMS_PATH)
    write_face_list_items_sql(item_records, SQL_FACE_LIST_ITEMS_PATH)

    write_mapping(
        [
            {
                "old_id": record.old_id,
                "new_id": record.id,
                "name": record.name,
                "old_list_id": record.old_list_id,
                "new_list_id": record.list_id,
                "old_client_id": record.old_client_id,
                "new_client_id": record.client_id,
                "old_created_by": record.old_created_by,
                "new_created_by": record.created_by,
            }
            for record in item_records
        ],
        item_unmapped_old,
        item_unmapped_new,
        [
            "legacy status != -1 items remapped sequentially to id",
            "list_id remapped via face_lists.json",
            "client_id remapped via clients.json",
            "created_by remapped via users.json (fallback to admin id 1)",
        ],
        MAP_FACE_LIST_ITEMS_PATH,
    )

    # Face list item images
    image_rows = parse_pipe_table(OLD_FACE_LIST_ITEMS_IMAGES_PATH)
    image_headers, image_existing_lines, image_existing_rows, image_max_id = parse_existing_dataset(
        NEW_FACE_LIST_ITEMS_IMAGES_PATH
    )
    image_records, image_unmapped_old, image_unmapped_new = build_face_list_item_images(
        image_rows, item_id_map, item_to_list_map, file_index, list_folder_map, item_slug_map, image_max_id
    )

    write_face_list_item_images_dataset(
        image_headers, image_existing_lines, image_records, NEW_FACE_LIST_ITEMS_IMAGES_PATH
    )
    write_face_list_items_images_sql(image_records, SQL_FACE_LIST_ITEMS_IMAGES_PATH)

    image_row_by_id = {
        to_int(row.get("id")): row for row in image_rows if to_int(row.get("id")) is not None
    }
    write_mapping(
        [
            {
                "old_id": record.old_id,
                "new_id": record.id,
                "old_list_item_id": record.old_list_item_id,
                "new_list_item_id": record.list_item_id,
                "old_path": strip_outer_quotes(image_row_by_id.get(record.old_id, {}).get("path")),
                "new_path": record.path,
            }
            for record in image_records
        ],
        image_unmapped_old,
        image_unmapped_new,
        [
            "list_item_id remapped via face_list_items.json",
            "paths rewritten to per-list folders with slugified item names",
        ],
        MAP_FACE_LIST_ITEMS_IMAGES_PATH,
    )


if __name__ == "__main__":
    main()
