#!/usr/bin/env bash
# Rename and relocate face list images according to docs/face_list_items_images.json.
# Requires jq. Source images expected under ./face_lists; outputs under ./face_lists_new.

set -euo pipefail

MANIFEST="docs/face_list_items_images.json"
SOURCE_DIR="face_lists"
TARGET_ROOT="face_lists_new"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to run this script." >&2
  exit 1
fi

if [[ ! -f "${MANIFEST}" ]]; then
  echo "Manifest not found at ${MANIFEST}" >&2
  exit 1
fi

if [[ ! -d "${SOURCE_DIR}" ]]; then
  echo "Source directory ${SOURCE_DIR} is missing" >&2
  exit 1
fi

mkdir -p "${TARGET_ROOT}"

tmpfile="$(mktemp)"
trap 'rm -f "${tmpfile}"' EXIT

jq -c '.items[] | {dir: .target_dir, name: .name_sanitized, images: .images}' "${MANIFEST}" > "${tmpfile}"

while IFS= read -r line; do
  dir="$(printf '%s' "${line}" | jq -r '.dir')"
  name="$(printf '%s' "${line}" | jq -r '.name')"
  mkdir -p "${TARGET_ROOT}/${dir}"

  printf '%s' "${line}" | jq -r '.images[]' | while IFS= read -r image; do
    src="${SOURCE_DIR}/${image}"
    if [[ ! -f "${src}" ]]; then
      echo "Warning: missing source ${src}" >&2
      continue
    fi

    ext="${src##*.}"
    dest="${TARGET_ROOT}/${dir}/${name}.${ext}"

    # Avoid overwriting distinct files for the same person; add numeric suffix if needed.
    if [[ -f "${dest}" ]]; then
      idx=1
      while [[ -f "${TARGET_ROOT}/${dir}/${name}_${idx}.${ext}" ]]; do
        ((idx++))
      done
      dest="${TARGET_ROOT}/${dir}/${name}_${idx}.${ext}"
    fi

    mv "${src}" "${dest}"
  done
done < "${tmpfile}"

echo "Image renaming and relocation complete."
