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

# Build a lookup from image filename -> {name, dir}.
jq -c '.items[] | {image: .images[], dir: .target_dir, name: .name_sanitized}' "${MANIFEST}" > "${tmpfile}"

declare -A IMAGE_TO_NAME
declare -A IMAGE_TO_DIR

while IFS= read -r line; do
  image="$(printf '%s' "${line}" | jq -r '.image')"
  dir="$(printf '%s' "${line}" | jq -r '.dir')"
  name="$(printf '%s' "${line}" | jq -r '.name')"

  if [[ -n "${IMAGE_TO_NAME[${image}]:-}" ]]; then
    echo "Warning: duplicate mapping for ${image}; keeping first entry (${IMAGE_TO_DIR[${image}]}) and ignoring ${dir}" >&2
    continue
  fi

  IMAGE_TO_NAME["${image}"]="${name}"
  IMAGE_TO_DIR["${image}"]="${dir}"
done < "${tmpfile}"

shopt -s nullglob
moved=0
unmapped=0

for src in "${SOURCE_DIR}"/*; do
  [[ -f "${src}" ]] || continue

  filename="$(basename "${src}")"
  name="${IMAGE_TO_NAME[${filename}]:-}"
  dir="${IMAGE_TO_DIR[${filename}]:-}"

  if [[ -z "${name}" || -z "${dir}" ]]; then
    echo "Warning: no mapping found for ${filename}; skipping" >&2
    ((unmapped++))
    continue
  fi

  mkdir -p "${TARGET_ROOT}/${dir}"

  ext="${filename##*.}"
  dest="${TARGET_ROOT}/${dir}/${name}.${ext}"

  # Avoid overwriting distinct files for the same person; add numeric suffix if needed.
  if [[ -e "${dest}" ]]; then
    idx=1
    while [[ -e "${TARGET_ROOT}/${dir}/${name}_${idx}.${ext}" ]]; do
      ((idx++))
    done
    dest="${TARGET_ROOT}/${dir}/${name}_${idx}.${ext}"
  fi

  mv "${src}" "${dest}"
  ((moved++))
done

echo "Completed. Moved ${moved} files; ${unmapped} files had no mapping."
