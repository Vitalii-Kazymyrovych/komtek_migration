#!/usr/bin/env python3
"""Event manager migration helper.

Reads legacy event_manager data, normalizes text fields to ASCII, remaps
client_ids using the existing clients mapping, and regenerates:
  - new_dataset/event_manager_202512301039.txt
  - sql/event_manager_inserts.sql
  - maps/event_manager.json
"""
from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


OLD_EVENTS_PATH = Path("old_dataset/_event_manager__202512301049.txt")
NEW_EVENTS_PATH = Path("new_dataset/event_manager_202512301039.txt")
CLIENT_MAP_PATH = Path("maps/clients.json")
SQL_OUTPUT_PATH = Path("sql/event_manager_inserts.sql")
MAP_OUTPUT_PATH = Path("maps/event_manager.json")

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
class EventManagerRecord:
    """Normalized event manager record with remapped client reference."""

    id: int
    old_id: str
    uuid: str
    title: Optional[str]
    description: Optional[str]
    created_at: Optional[str]
    nodes: Optional[str]
    client_id: int
    old_client_id: int


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


def decode_nodes(value: Optional[str]) -> Optional[str]:
    """Decode the escaped nodes string without altering its structure."""

    cleaned = clean_value(value)
    if cleaned is None:
        return None

    unquoted = strip_outer_quotes(cleaned) or ""
    try:
        return unquoted.encode("utf-8").decode("unicode_escape")
    except Exception:
        return unquoted


def to_int(value: Optional[str]) -> Optional[int]:
    """Convert a numeric-looking string to int, removing thousands separators."""

    cleaned = clean_value(value)
    if cleaned is None:
        return None
    numeric = cleaned.replace(",", "")
    return int(numeric)


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


def parse_existing_dataset(path: Path) -> Tuple[Sequence[str], List[str], List[Dict[str, Any]], int]:
    """Read the current new_dataset file, preserving existing rows and ids."""

    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 2:
        raise ValueError("Existing event_manager dataset is missing header rows.")

    header_lines = lines[:2]
    data_lines = [line for line in lines[2:] if line.strip()]

    headers = [header.strip() for header in header_lines[0].strip("|").split("|")]
    parsed_existing: List[Dict[str, Any]] = []
    max_id = 0

    for line in data_lines:
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        row = dict(zip(headers, cells))
        try:
            row_id = int(row["id"].replace(",", ""))
        except Exception:
            continue
        max_id = max(max_id, row_id)
        parsed_existing.append(
            {
                "id": row_id,
                "uuid": row.get("uuid"),
                "title": row.get("title"),
                "client_id": row.get("client_id"),
                "line": line,
            }
        )

    parsed_existing.sort(key=lambda entry: entry["id"])
    preserved_lines = [entry["line"] for entry in parsed_existing]
    unmapped_new = [
        {
            "new_id": entry["id"],
            "new_uuid": entry.get("uuid"),
            "title": normalize_text(entry.get("title")) or entry.get("title"),
            "client_id": to_int(entry.get("client_id")),
            "reason": "pre-existing new_dataset row",
        }
        for entry in parsed_existing
    ]

    return header_lines, preserved_lines, unmapped_new, max_id


def load_client_map(path: Path) -> Dict[int, int]:
    """Load an old->new client_id mapping from an existing mapping file."""

    mapping_data = json.loads(path.read_text(encoding="utf-8"))
    return {entry["old_id"]: entry["new_id"] for entry in mapping_data.get("mapped", [])}


def build_records(
    rows: List[Dict[str, Optional[str]]], client_map: Dict[int, int], starting_id: int
) -> tuple[List[EventManagerRecord], List[Dict[str, Any]]]:
    """Normalize and remap rows, preserving deterministic ordering."""

    records: List[EventManagerRecord] = []
    unmapped_old: List[Dict[str, Any]] = []
    next_id = starting_id

    for row in rows:
        old_uuid = strip_outer_quotes(clean_value(row.get("id")))
        client_id = to_int(row.get("client_id"))

        if old_uuid is None:
            unmapped_old.append(
                {
                    "old_id": row.get("id"),
                    "title": normalize_text(strip_outer_quotes(row.get("title")))
                    or row.get("title"),
                    "old_client_id": client_id,
                    "reason": "missing legacy uuid",
                }
            )
            continue

        if client_id is None or client_id not in client_map:
            unmapped_old.append(
                {
                    "old_id": old_uuid,
                    "title": normalize_text(strip_outer_quotes(row.get("title")))
                    or row.get("title"),
                    "old_client_id": client_id,
                    "reason": "client_id missing from clients mapping",
                }
            )
            continue

        record = EventManagerRecord(
            id=next_id,
            old_id=old_uuid,
            uuid=old_uuid,
            title=normalize_text(strip_outer_quotes(row.get("title"))),
            description=normalize_text(strip_outer_quotes(row.get("description"))),
            created_at=clean_value(row.get("created_at")),
            nodes=decode_nodes(row.get("nodes")),
            client_id=client_map[client_id],
            old_client_id=client_id,
        )
        records.append(record)
        next_id += 1

    records.sort(key=lambda record: record.id)
    unmapped_old.sort(key=lambda record: (record.get("old_id") or ""))
    return records, unmapped_old


def format_cell(value: Optional[Any]) -> str:
    """Serialize a dataset cell to pipe-table format."""

    if value is None:
        return "[NULL]"
    return str(value)


def write_new_dataset(
    header_lines: Sequence[str], existing_lines: List[str], records: List[EventManagerRecord], path: Path
) -> None:
    """Write the merged event_manager table to the new dataset file."""

    lines = list(header_lines) + list(existing_lines)
    for record in records:
        lines.append(
            "|".join(
                [
                    "",
                    format_cell(record.id),
                    format_cell(record.uuid),
                    format_cell(record.title),
                    format_cell(record.description),
                    format_cell(record.created_at),
                    format_cell(record.nodes),
                    f"{format_cell(record.client_id)}",
                    "",
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


def sql_numeric(value: Optional[Any]) -> str:
    """Format numeric values for SQL output."""

    if value is None:
        return "NULL"
    return str(value)


def write_sql(records: List[EventManagerRecord], path: Path) -> None:
    """Generate a batched INSERT statement for event_manager."""

    value_lines = []
    for record in records:
        value_lines.append(
            "  ("
            + ", ".join(
                [
                    sql_numeric(record.id),
                    sql_string(record.uuid),
                    sql_string(record.title),
                    sql_string(record.description),
                    sql_string(record.created_at),
                    sql_string(record.nodes),
                    sql_numeric(record.client_id),
                ]
            )
            + ")"
        )

    if not value_lines:
        path.write_text("-- No event_manager rows to insert.\n", encoding="utf-8")
        return

    sql = (
        'INSERT INTO videoanalytics.event_manager (id, "uuid", title, description, created_at, nodes, client_id)\nVALUES\n'
        + ",\n".join(value_lines)
        + ";\n"
    )
    path.write_text(sql, encoding="utf-8")


def write_mapping(
    records: List[EventManagerRecord],
    unmapped_old: List[Dict[str, Any]],
    unmapped_new: List[Dict[str, Any]],
    path: Path,
) -> None:
    """Create the mapping JSON artifact."""

    mapping = {
        "match_keys": [
            "legacy id to uuid",
            "title (ASCII normalized) to title",
            "created_at to created_at",
            "client_id remapped via clients.json to client_id",
        ],
        "mapped": [
            {
                "old_id": record.old_id,
                "new_id": record.id,
                "old_uuid": record.old_id,
                "new_uuid": record.uuid,
                "title": record.title,
                "old_client_id": record.old_client_id,
                "new_client_id": record.client_id,
                "created_at": record.created_at,
            }
            for record in records
        ],
        "unmapped_old": unmapped_old,
        "unmapped_new": unmapped_new,
    }
    path.write_text(json.dumps(mapping, indent=2), encoding="utf-8")


def main() -> None:
    client_map = load_client_map(CLIENT_MAP_PATH)
    header_lines, existing_lines, unmapped_new, max_existing_id = parse_existing_dataset(
        NEW_EVENTS_PATH
    )

    legacy_rows = parse_pipe_table(OLD_EVENTS_PATH)
    starting_id = max_existing_id + 1
    records, unmapped_old = build_records(legacy_rows, client_map, starting_id)

    write_new_dataset(header_lines, existing_lines, records, NEW_EVENTS_PATH)
    write_sql(records, SQL_OUTPUT_PATH)
    write_mapping(records, unmapped_old, unmapped_new, MAP_OUTPUT_PATH)
    print(
        f"Processed {len(records)} mapped event_manager rows (starting id {starting_id}), "
        f"{len(unmapped_old)} unmapped legacy, {len(unmapped_new)} preserved existing."
    )


if __name__ == "__main__":
    main()
