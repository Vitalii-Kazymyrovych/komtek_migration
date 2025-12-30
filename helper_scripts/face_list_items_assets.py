#!/usr/bin/env python3
"""Export face list items and related images into a structured manifest.

Reads legacy face list items and image metadata, filters out disabled items
(`status = -1`), normalizes names, and produces:
  - docs/face_list_items_images.json — manifest tying items to image files and
    target directories under face_lists_new
  - face_lists_new/<list_dir>/.gitkeep placeholders for each active list

This helper keeps the asset preparation reproducible and deterministic.
"""

from __future__ import annotations

import json
import unicodedata
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


OLD_ITEMS_PATH = Path("old_dataset/_face_list_items__202512301049.txt")
OLD_IMAGES_PATH = Path("old_dataset/_face_list_items_images__202512301049.txt")
FACE_LIST_MAP_PATH = Path("maps/face_lists.json")
OUTPUT_PATH = Path("docs/face_list_items_images.json")
FACE_LISTS_NEW_ROOT = Path("face_lists_new")


PLACEHOLDER_NULLS = {"", "-", "NULL", "null", "[NULL]"}

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


def ensure_ascii(text: Optional[str]) -> Optional[str]:
    """Normalize text to ASCII, dropping diacritics."""

    if text is None:
        return None

    trimmed = text.strip()
    if trimmed in PLACEHOLDER_NULLS:
        return None

    substituted = trimmed.translate(SUBSTITUTIONS)
    normalized = unicodedata.normalize("NFKD", substituted)
    return normalized.encode("ascii", "ignore").decode("ascii")


def sanitize_for_filename(text: Optional[str], fallback: str) -> str:
    """Make a filesystem-friendly ASCII filename chunk."""

    base = ensure_ascii(text) or fallback
    safe_chars = []
    for char in base:
        if char.isalnum():
            safe_chars.append(char)
        elif char in {" ", "-", "_", "."}:
            safe_chars.append("_")
    cleaned = "".join(safe_chars) or fallback
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_")


def parse_pipe_table(path: Path) -> Tuple[Sequence[str], List[Dict[str, Optional[str]]]]:
    """Parse a pipe-delimited table, returning headers and row dicts."""

    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 2:
        return [], []

    headers = [header.strip() for header in lines[0].strip("|").split("|")]
    rows: List[Dict[str, Optional[str]]] = []
    for raw in lines[2:]:
        if not raw.strip():
            continue
        cells = [cell.strip() for cell in raw.strip("|").split("|")]
        if len(cells) < len(headers):
            cells.extend([None] * (len(headers) - len(cells)))
        rows.append(dict(zip(headers, cells)))
    return headers, rows


def parse_int(value: Optional[str]) -> Optional[int]:
    """Parse an integer safely."""

    if value is None:
        return None
    cleaned = value.strip()
    if cleaned in PLACEHOLDER_NULLS:
        return None
    try:
        return int(cleaned)
    except Exception:
        return None


def load_face_list_mapping(path: Path) -> Dict[int, Dict[str, Optional[str]]]:
    """Load old->new face list mapping for name lookups."""

    data = json.loads(path.read_text(encoding="utf-8"))
    mapping: Dict[int, Dict[str, Optional[str]]] = {}
    for entry in data.get("mapped", []):
        old_id = entry.get("old_id")
        if old_id is None:
            continue
        mapping[int(old_id)] = {
            "new_id": entry.get("new_id"),
            "name": entry.get("name"),
        }
    return mapping


def extract_items(rows: Iterable[Dict[str, Optional[str]]]) -> Dict[int, Dict[str, Optional[str]]]:
    """Collect active face list items keyed by item id."""

    items: Dict[int, Dict[str, Optional[str]]] = {}
    for row in rows:
        item_id = parse_int(row.get("id"))
        status = parse_int(row.get("status")) or 0
        if item_id is None or status == -1:
            continue

        items[item_id] = {
            "name": ensure_ascii(row.get("name")),
            "status": status,
            "list_id": parse_int(row.get("list_id")),
        }
    return items


def extract_images(rows: Iterable[Dict[str, Optional[str]]]) -> Dict[int, List[str]]:
    """Group image basenames by list_item_id."""

    images: Dict[int, List[str]] = {}
    for row in rows:
        list_item_id = parse_int(row.get("list_item_id"))
        raw_path = row.get("path") or ""
        path = raw_path.strip().strip('"')
        basename = Path(path).name
        if list_item_id is None or not basename:
            continue
        images.setdefault(list_item_id, []).append(basename)
    for file_list in images.values():
        file_list.sort()
    return images


def build_manifest(
    items: Dict[int, Dict[str, Optional[str]]],
    images: Dict[int, List[str]],
    face_list_map: Dict[int, Dict[str, Optional[str]]],
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]], List[str]]:
    """Prepare manifest entries and the directories that need to exist."""

    entries: List[Dict[str, object]] = []
    unmapped_lists: List[Dict[str, object]] = []
    directories: List[str] = []

    for item_id in sorted(items):
        item = items[item_id]
        list_id = item.get("list_id")
        list_meta = face_list_map.get(list_id or -1)

        list_name = ensure_ascii(list_meta.get("name")) if list_meta else None
        list_new_id = list_meta.get("new_id") if list_meta else None

        if list_meta is None:
            unmapped_lists.append({
                "list_id": list_id,
                "list_item_id": item_id,
                "item_name": item.get("name"),
                "reason": "list_id not present in face_lists mapping",
            })

        target_dir = "list_{0}".format(list_id if list_id is not None else "unknown")
        if list_name:
            target_dir = f"{sanitize_for_filename(list_name, target_dir)}__old{list_id}"
            if list_new_id is not None:
                target_dir += f"_new{list_new_id}"

        item_name = item.get("name") or ""
        safe_item_name = sanitize_for_filename(item_name, f"item_{item_id}")
        entry_images = images.get(item_id, [])

        entries.append(
            {
                "list_item_id": item_id,
                "name": item_name,
                "name_sanitized": safe_item_name,
                "status": item.get("status"),
                "list_id": list_id,
                "list_name": list_name,
                "list_new_id": list_new_id,
                "target_dir": target_dir,
                "images": entry_images,
            }
        )
        directories.append(target_dir)

    return entries, unmapped_lists, sorted(set(directories))


def write_manifest(
    entries: List[Dict[str, object]],
    unmapped_lists: List[Dict[str, object]],
    directories: Sequence[str],
) -> None:
    """Persist manifest JSON and ensure directory skeleton exists."""

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    FACE_LISTS_NEW_ROOT.mkdir(parents=True, exist_ok=True)

    for directory in directories:
        dest = FACE_LISTS_NEW_ROOT / directory
        dest.mkdir(parents=True, exist_ok=True)
        gitkeep = dest / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.write_text("placeholder", encoding="utf-8")

    manifest = {
        "match_keys": [
            "list_item_id to list_item_id",
            "status != -1",
            "path basename from face_list_items_images to images",
            "list_id to face_lists mapping",
        ],
        "items": entries,
        "unmapped_lists": unmapped_lists,
    }

    OUTPUT_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def main() -> None:
    _, item_rows = parse_pipe_table(OLD_ITEMS_PATH)
    _, image_rows = parse_pipe_table(OLD_IMAGES_PATH)

    items = extract_items(item_rows)
    images = extract_images(image_rows)
    face_list_map = load_face_list_mapping(FACE_LIST_MAP_PATH)

    entries, unmapped_lists, directories = build_manifest(items, images, face_list_map)
    write_manifest(entries, unmapped_lists, directories)


if __name__ == "__main__":
    main()
