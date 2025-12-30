#!/usr/bin/env python3
"""Analytics migration helper.

Creates plugin-scoped analytics groups cloned from stream groups and migrates
legacy analytics entries while remapping client, stream, and creator
references. Generates:
  - new_dataset/analytics_groups_202512301039.txt
  - new_dataset/analytics_202512301039.txt (merged with existing rows)
  - sql/analytics_groups_inserts.sql
  - sql/analytics_inserts.sql
  - maps/analytics_groups.json
  - maps/analytics.json
"""
from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

OLD_ANALYTICS_PATH = Path("old_dataset/_analytics__202512301049.txt")
NEW_ANALYTICS_PATH = Path("new_dataset/analytics_202512301039.txt")
ANALYTICS_GROUPS_PATH = Path("new_dataset/analytics_groups_202512301039.txt")
STREAM_GROUP_MAP_PATH = Path("maps/stream_groups.json")
STREAM_MAP_PATH = Path("maps/streams.json")
CLIENT_MAP_PATH = Path("maps/clients.json")
USER_MAP_PATH = Path("maps/users.json")
SQL_ANALYTICS_GROUPS_PATH = Path("sql/analytics_groups_inserts.sql")
SQL_ANALYTICS_PATH = Path("sql/analytics_inserts.sql")
ANALYTICS_GROUP_MAP_OUTPUT = Path("maps/analytics_groups.json")
ANALYTICS_MAP_OUTPUT = Path("maps/analytics.json")

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
        raise ValueError("Existing analytics dataset is missing header rows.")

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
        parsed_rows.append(
            {
                "id": row_id,
                "plugin_name": row.get("plugin_name"),
                "name": row.get("name"),
            }
        )

    return header_lines, data_lines, parsed_rows, max_id


def load_id_map(path: Path) -> Dict[int, int]:
    """Load an old->new id mapping from an existing mapping file."""
    mapping_data = json.loads(path.read_text(encoding="utf-8"))
    return {entry["old_id"]: entry["new_id"] for entry in mapping_data.get("mapped", [])}


def load_stream_details(path: Path) -> Dict[int, Dict[str, Optional[str]]]:
    """Load stream metadata from the new_dataset for uuid lookups."""
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 2:
        raise ValueError("Streams dataset missing header rows.")
    headers = [h.strip() for h in lines[0].strip("|").split("|")]
    details: Dict[int, Dict[str, Optional[str]]] = {}
    for line in lines[2:]:
        if not line.strip():
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        row = dict(zip(headers, cells))
        try:
            row_id = int(row["id"].replace(",", ""))
        except Exception:
            continue
        details[row_id] = {"uuid": clean_value(row.get("uuid"))}
    return details


@dataclass
class AnalyticsGroup:
    """Plugin-scoped analytics group cloned from a stream group."""

    id: int
    old_stream_group_id: int
    name: str
    parent_id: int
    plugin_name: str
    client_id: int
    old_client_id: int


def build_analytics_groups(
    plugin_names: Iterable[str], stream_group_entries: List[Dict[str, Any]]
) -> Tuple[List[AnalyticsGroup], Dict[Tuple[str, int], int]]:
    """Create plugin-specific analytics groups for every stream group."""
    groups: List[AnalyticsGroup] = []
    group_lookup: Dict[Tuple[str, int], int] = {}
    next_id = 1

    for plugin_name in sorted(plugin_names):
        for entry in sorted(stream_group_entries, key=lambda e: e["new_id"]):
            group = AnalyticsGroup(
                id=next_id,
                old_stream_group_id=entry["old_id"],
                name=entry["name"],
                parent_id=entry["new_parent_id"],
                plugin_name=plugin_name,
                client_id=entry["new_client_id"],
                old_client_id=entry["old_client_id"],
            )
            groups.append(group)
            group_lookup[(plugin_name, entry["old_id"])] = next_id
            next_id += 1

    return groups, group_lookup


@dataclass
class AnalyticsRecord:
    """Normalized analytics record with remapped references."""

    id: int
    old_id: int
    uuid: str
    type: str
    plugin_name: str
    name: str
    created_at: Optional[str]
    status: str
    client_id: int
    old_client_id: int
    stream: Optional[str]
    module: Optional[Dict[str, Any]]
    last_gpu_id: Optional[int]
    desired_server_id: Optional[int]
    disable_balancing: Optional[Any]
    start_signature: Optional[str]
    allowed_server_ids: Optional[Any]
    restrictions: Optional[Dict[str, Any]]
    events_holder: Optional[Dict[str, Any]]
    start_at: Optional[str]
    stream_uuid: Optional[str]
    group_id: int
    old_stream_id: int
    new_stream_id: int
    old_stream_group_id: int
    old_creator_id: Optional[int]
    new_creator_id: Optional[int]


def build_analytics_records(
    rows: List[Dict[str, Optional[str]]],
    client_map: Dict[int, int],
    stream_map: Dict[int, Dict[str, Any]],
    analytics_group_lookup: Dict[Tuple[str, int], int],
    user_map: Dict[int, int],
    stream_details: Dict[int, Dict[str, Optional[str]]],
    starting_id: int,
) -> tuple[List[AnalyticsRecord], List[Dict[str, Any]]]:
    """Normalize and remap analytics rows, preserving deterministic ordering."""
    records: List[AnalyticsRecord] = []
    unmapped_old: List[Dict[str, Any]] = []
    next_id = starting_id

    for row in sorted(rows, key=lambda r: to_int(r.get("id")) or 0):
        old_id = to_int(row.get("id")) or 0
        status_text = strip_outer_quotes(row.get("status"))
        if to_int(status_text) == -1:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "name": normalize_text(row.get("name")) or row.get("name"),
                    "reason": "status = -1 (excluded)",
                }
            )
            continue

        plugin_name = normalize_text(strip_outer_quotes(row.get("plugin_name"))) or ""
        client_id = to_int(row.get("client_id"))
        stream_id = to_int(row.get("stream_id"))

        if client_id is None or client_id not in client_map:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "plugin_name": plugin_name,
                    "old_client_id": client_id,
                    "old_stream_id": stream_id,
                    "reason": "client_id missing from clients mapping",
                }
            )
            continue

        if stream_id is None or stream_id not in stream_map:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "plugin_name": plugin_name,
                    "old_client_id": client_id,
                    "old_stream_id": stream_id,
                    "reason": "stream_id missing from streams mapping",
                }
            )
            continue

        stream_mapping = stream_map[stream_id]
        stream_group_id = stream_mapping["old_parent_id"]
        if stream_group_id not in (0,) and (plugin_name, stream_group_id) not in analytics_group_lookup:
            unmapped_old.append(
                {
                    "old_id": old_id,
                    "plugin_name": plugin_name,
                    "old_client_id": client_id,
                    "old_stream_id": stream_id,
                    "old_stream_group_id": stream_group_id,
                    "reason": "analytics group mapping missing for stream group/plugin",
                }
            )
            continue

        restrictions = parse_json_field(row.get("restrictions"))
        old_creator_id = restrictions.get("creator_id") if restrictions else None
        new_creator_id = user_map.get(old_creator_id) if old_creator_id is not None else None
        if restrictions is not None:
            restrictions = {**restrictions, "creator_id": new_creator_id}

        record = AnalyticsRecord(
            id=next_id,
            old_id=old_id,
            uuid=normalize_text(strip_outer_quotes(row.get("topic"))) or "",
            type=normalize_text(strip_outer_quotes(row.get("type"))) or "",
            plugin_name=plugin_name,
            name=normalize_text(strip_outer_quotes(row.get("name"))) or "",
            created_at=clean_value(row.get("created_at")),
            status=status_text or "",
            client_id=client_map[client_id],
            old_client_id=client_id,
            stream=None,
            module=parse_json_field(row.get("module")),
            last_gpu_id=to_int(row.get("last_gpu_id")),
            desired_server_id=to_int(row.get("desired_server_id")),
            disable_balancing=parse_json_field(row.get("disable_balancing")),
            start_signature=clean_value(strip_outer_quotes(row.get("start_signature"))),
            allowed_server_ids=parse_json_field(row.get("allowed_server_ids")),
            restrictions=restrictions,
            events_holder=parse_json_field(row.get("events_holder")),
            start_at=clean_value(row.get("start_at")),
            stream_uuid=stream_details.get(stream_mapping["new_id"], {}).get("uuid"),
            group_id=0 if stream_group_id in (0,) else analytics_group_lookup[(plugin_name, stream_group_id)],
            old_stream_id=stream_id,
            new_stream_id=stream_mapping["new_id"],
            old_stream_group_id=stream_group_id,
            old_creator_id=old_creator_id,
            new_creator_id=new_creator_id,
        )
        records.append(record)
        next_id += 1

    records.sort(key=lambda record: record.id)
    unmapped_old.sort(key=lambda record: record["old_id"])
    return records, unmapped_old


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


def write_analytics_groups_dataset(groups: List[AnalyticsGroup], path: Path) -> None:
    """Write the analytics_groups table to the new dataset file."""
    lines = ["|id |name|parent_id|plugin_name|client_id|", "|---|----|---------|-----------|---------|"]
    for group in groups:
        lines.append(
            f"|{group.id}|{group.name}|{group.parent_id}|{group.plugin_name}|{group.client_id}|"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_analytics_groups_sql(groups: List[AnalyticsGroup], path: Path) -> None:
    """Generate a batched INSERT statement for analytics_groups."""
    value_lines = []
    for group in groups:
        name_sql = group.name.replace("'", "''")
        plugin_sql = group.plugin_name.replace("'", "''")
        value_lines.append(
            f"  ({group.id}, '{name_sql}', {group.parent_id}, '{plugin_sql}', {group.client_id})"
        )
    sql = (
        'INSERT INTO videoanalytics.analytics_groups (id, "name", parent_id, plugin_name, client_id)\nVALUES\n'
        + ",\n".join(value_lines)
        + ";\n"
    )
    path.write_text(sql, encoding="utf-8")


def write_analytics_groups_mapping(
    groups: List[AnalyticsGroup], path: Path
) -> None:
    """Create the analytics_groups mapping JSON artifact."""
    mapping = {
        "match_keys": [
            "stream_group.id cloned per plugin_name to analytics_group.id",
            "name preserved from stream_groups (ASCII normalized)",
            "client_id remapped via clients.json to client_id",
            "parent_id preserved from stream_groups parent_id",
        ],
        "mapped": [
            {
                "old_stream_group_id": group.old_stream_group_id,
                "new_id": group.id,
                "plugin_name": group.plugin_name,
                "name": group.name,
                "old_client_id": group.old_client_id,
                "new_client_id": group.client_id,
                "old_parent_id": 0,
                "new_parent_id": group.parent_id,
            }
            for group in groups
        ],
        "unmapped_old": [],
        "unmapped_new": [],
    }
    path.write_text(json.dumps(mapping, indent=2), encoding="utf-8")


def write_analytics_dataset(
    header_lines: Sequence[str],
    existing_lines: List[str],
    records: List[AnalyticsRecord],
    path: Path,
) -> None:
    """Write the merged analytics table to the new dataset file."""
    lines = list(header_lines) + list(existing_lines)
    for record in records:
        lines.append(
            "|".join(
                [
                    f"|{format_cell(record.id)}",
                    format_cell(record.uuid),
                    format_cell(record.type),
                    format_cell(record.plugin_name),
                    format_cell(record.name),
                    format_cell(record.created_at),
                    format_cell(record.status),
                    format_cell(record.client_id),
                    format_cell(record.stream),
                    format_json_field(record.module),
                    format_cell(record.last_gpu_id),
                    format_cell(record.desired_server_id),
                    format_cell(record.disable_balancing),
                    format_cell(record.start_signature),
                    format_json_field(record.allowed_server_ids),
                    format_json_field(record.restrictions),
                    format_json_field(record.events_holder),
                    format_cell(record.start_at),
                    format_cell(record.stream_uuid),
                    f"{format_cell(record.group_id)}|",
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


def write_analytics_sql(records: List[AnalyticsRecord], path: Path) -> None:
    """Generate a batched INSERT statement for analytics."""
    value_lines = []
    for record in records:
        value_lines.append(
            "  ("
            + ", ".join(
                [
                    sql_numeric(record.id),
                    sql_string(record.uuid),
                    sql_string(record.type),
                    sql_string(record.plugin_name),
                    sql_string(record.name),
                    sql_string(record.created_at),
                    sql_string(record.status),
                    sql_numeric(record.client_id),
                    sql_string(record.stream),
                    sql_json(record.module),
                    sql_numeric(record.last_gpu_id),
                    sql_numeric(record.desired_server_id),
                    sql_numeric(record.disable_balancing),
                    sql_string(record.start_signature),
                    sql_json(record.allowed_server_ids),
                    sql_json(record.restrictions),
                    sql_json(record.events_holder),
                    sql_string(record.start_at),
                    sql_string(record.stream_uuid),
                    sql_numeric(record.group_id),
                ]
            )
            + ")"
        )

    sql = (
        'INSERT INTO videoanalytics.analytics (id, uuid, type, plugin_name, "name", created_at, status, client_id, stream, module, last_gpu_id, desired_server_id, disable_balancing, start_signature, allowed_server_ids, restrictions, events_holder, start_at, stream_uuid, group_id)\nVALUES\n'
        + ",\n".join(value_lines)
        + ";\n"
    )
    path.write_text(sql, encoding="utf-8")


def write_analytics_mapping(
    records: List[AnalyticsRecord],
    unmapped_old: List[Dict[str, Any]],
    unmapped_new: List[Dict[str, Any]],
    path: Path,
) -> None:
    """Create the analytics mapping JSON artifact."""
    mapping = {
        "match_keys": [
            "legacy analytics ids remapped sequentially after existing new_dataset ids",
            "client_id remapped via clients.json to client_id",
            "stream_id remapped via streams.json to stream_uuid (stream_uuid column) and new stream id",
            "stream_group_id + plugin_name remapped via analytics_groups.json to group_id",
            "restrictions.creator_id remapped via users.json to restrictions.creator_id",
        ],
        "mapped": [
            {
                "old_id": record.old_id,
                "new_id": record.id,
                "plugin_name": record.plugin_name,
                "name": record.name,
                "old_client_id": record.old_client_id,
                "new_client_id": record.client_id,
                "old_stream_id": record.old_stream_id,
                "new_stream_id": record.new_stream_id,
                "old_stream_group_id": record.old_stream_group_id,
                "new_group_id": record.group_id,
                "old_creator_id": record.old_creator_id,
                "new_creator_id": record.new_creator_id,
                "status": record.status,
            }
            for record in records
        ],
        "unmapped_old": unmapped_old,
        "unmapped_new": unmapped_new,
    }
    path.write_text(json.dumps(mapping, indent=2), encoding="utf-8")


def load_stream_group_entries(path: Path) -> List[Dict[str, Any]]:
    """Load stream group mapping entries."""
    data = json.loads(path.read_text(encoding="utf-8"))
    return sorted(data.get("mapped", []), key=lambda entry: entry["new_id"])


def main() -> None:
    client_map = load_id_map(CLIENT_MAP_PATH)
    stream_group_entries = load_stream_group_entries(STREAM_GROUP_MAP_PATH)
    stream_map_entries = json.loads(STREAM_MAP_PATH.read_text(encoding="utf-8"))
    stream_map = {entry["old_id"]: entry for entry in stream_map_entries.get("mapped", [])}
    user_map = load_id_map(USER_MAP_PATH)
    stream_details = load_stream_details(Path("new_dataset/streams_202512301039.txt"))

    legacy_rows = parse_pipe_table(OLD_ANALYTICS_PATH)
    existing_header, existing_lines, existing_rows, max_existing_id = parse_existing_dataset(
        NEW_ANALYTICS_PATH
    )

    plugin_names = {
        normalize_text(strip_outer_quotes(row.get("plugin_name"))) or ""
        for row in legacy_rows
    }
    plugin_names.update(
        {normalize_text(clean_value(row.get("plugin_name"))) or "" for row in existing_rows}
    )
    plugin_names.discard("")

    analytics_groups, analytics_group_lookup = build_analytics_groups(plugin_names, stream_group_entries)
    write_analytics_groups_dataset(analytics_groups, ANALYTICS_GROUPS_PATH)
    write_analytics_groups_sql(analytics_groups, SQL_ANALYTICS_GROUPS_PATH)
    write_analytics_groups_mapping(analytics_groups, ANALYTICS_GROUP_MAP_OUTPUT)

    starting_id = max_existing_id + 1
    records, unmapped_old = build_analytics_records(
        legacy_rows,
        client_map,
        stream_map,
        analytics_group_lookup,
        user_map,
        stream_details,
        starting_id,
    )

    write_analytics_dataset(existing_header, existing_lines, records, NEW_ANALYTICS_PATH)
    write_analytics_sql(records, SQL_ANALYTICS_PATH)
    write_analytics_mapping(records, unmapped_old, existing_rows, ANALYTICS_MAP_OUTPUT)
    print(
        f"Processed {len(records)} mapped analytics (starting id {starting_id}), "
        f"{len(unmapped_old)} unmapped legacy, {len(existing_rows)} preserved existing."
    )


if __name__ == "__main__":
    main()
