#!/usr/bin/env python3
"""Stream groups migration helper.

Reads legacy stream_groups data, normalizes text fields, remaps foreign keys
using existing client mappings, and regenerates:
  - new_dataset/stream_groups_202512301039.txt
  - sql/stream_groups_inserts.sql
  - maps/stream_groups.json
"""
from __future__ import annotations

import json
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional


OLD_STREAM_GROUPS_PATH = Path("old_dataset/_stream_groups__202512301049.txt")
NEW_STREAM_GROUPS_PATH = Path("new_dataset/stream_groups_202512301039.txt")
CLIENT_MAP_PATH = Path("maps/clients.json")
SQL_OUTPUT_PATH = Path("sql/stream_groups_inserts.sql")
MAP_OUTPUT_PATH = Path("maps/stream_groups.json")

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


def parse_pipe_table(path: Path) -> List[Dict[str, Any]]:
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
        row: Dict[str, Any] = {}
        for header, cell in zip(headers, cells):
            value: Optional[str]
            if cell in {"", "-", "NULL", "null", "[NULL]"}:
                value = None
            else:
                value = cell

            if header in {"id", "parent_id", "client_id"}:
                row[header] = int(value) if value is not None else None
            else:
                row[header] = value

        data_rows.append(row)

    return data_rows


def load_client_mapping(path: Path) -> Dict[int, int]:
    """Load old->new client id mapping from the existing clients.json file."""
    mapping_data = json.loads(path.read_text(encoding="utf-8"))
    return {entry["old_id"]: entry["new_id"] for entry in mapping_data.get("mapped", [])}


def build_records(
    rows: List[Dict[str, Any]], client_map: Dict[int, int]
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Normalize and remap rows, preserving deterministic ordering.

    Returns a tuple of (mapped_records, unmapped_old_rows).
    """
    records: List[Dict[str, Any]] = []
    unmapped_old: List[Dict[str, Any]] = []
    for row in rows:
        client_id = row["client_id"]
        if client_id not in client_map:
            unmapped_old.append(
                {
                    "old_id": row["id"],
                    "name": normalize_text(row["name"]) or row["name"],
                    "old_client_id": client_id,
                    "parent_id": row["parent_id"],
                    "reason": "client_id missing from clients mapping",
                }
            )
            continue

        normalized_name = normalize_text(row["name"])
        if normalized_name is None:
            raise ValueError(f"Stream group name missing for id={row['id']}")

        record = {
            "id": row["id"],
            "old_id": row["id"],
            "parent_id": row["parent_id"],
            "old_parent_id": row["parent_id"],
            "client_id": client_map[client_id],
            "old_client_id": client_id,
            "name": normalized_name,
        }
        records.append(record)

    records.sort(key=lambda record: record["id"])
    unmapped_old.sort(key=lambda record: record["old_id"])
    return records, unmapped_old


def write_new_dataset(records: List[Dict[str, Any]], path: Path) -> None:
    """Write the normalized stream_groups table to the new dataset file."""
    lines = ["|id |parent_id|name|client_id|", "|---|---------|----|---------|"]
    for record in records:
        lines.append(
            f"|{record['id']}|{record['parent_id']}|{record['name']}|{record['client_id']}|"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_sql(records: List[Dict[str, Any]], path: Path) -> None:
    """Generate a batched INSERT statement for stream_groups."""
    value_lines = []
    for record in records:
        name_sql = record["name"].replace("'", "''")
        value_lines.append(
            f"  ({record['id']}, {record['parent_id']}, '{name_sql}', {record['client_id']})"
        )

    sql = (
        'INSERT INTO videoanalytics.stream_groups (id, parent_id, "name", client_id)\nVALUES\n'
        + ",\n".join(value_lines)
        + ";\n"
    )
    path.write_text(sql, encoding="utf-8")


def write_mapping(
    records: List[Dict[str, Any]], unmapped_old: List[Dict[str, Any]], path: Path
) -> None:
    """Create the mapping JSON artifact."""
    mapping = {
        "match_keys": [
            "id preserved 1:1 to id",
            "name (ASCII normalized) to name",
            "client_id remapped via clients.json to client_id",
            "parent_id preserved to parent_id",
        ],
        "mapped": [
            {
                "old_id": record["old_id"],
                "new_id": record["id"],
                "name": record["name"],
                "old_client_id": record["old_client_id"],
                "new_client_id": record["client_id"],
                "old_parent_id": record["old_parent_id"],
                "new_parent_id": record["parent_id"],
            }
            for record in records
        ],
        "unmapped_old": unmapped_old,
        "unmapped_new": [],
    }
    path.write_text(json.dumps(mapping, indent=2), encoding="utf-8")


def main() -> None:
    client_map = load_client_mapping(CLIENT_MAP_PATH)
    legacy_rows = parse_pipe_table(OLD_STREAM_GROUPS_PATH)
    records, unmapped_old = build_records(legacy_rows, client_map)

    write_new_dataset(records, NEW_STREAM_GROUPS_PATH)
    write_sql(records, SQL_OUTPUT_PATH)
    write_mapping(records, unmapped_old, MAP_OUTPUT_PATH)
    print(f"Processed {len(records)} mapped stream groups, {len(unmapped_old)} unmapped.")


if __name__ == "__main__":
    main()
