#!/usr/bin/env bash
# Rename and relocate face list images according to docs/face_list_items_images.json.
# Requires jq. Source images expected under ./face_lists; outputs under ./face_lists_new.

set -euo pipefail

MANIFEST="docs/face_list_items_images.json"
SOURCE_DIR="face_lists"
TARGET_ROOT="face_lists_new"
LOG_FILE="${LOG_FILE:-${TARGET_ROOT}/img_rename.log}"

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
mkdir -p "$(dirname "${LOG_FILE}")"
: > "${LOG_FILE}"

log() {
  local message="$*"
  local timestamp
  timestamp="$(date -Iseconds)"
  printf '%s %s\n' "${timestamp}" "${message}" | tee -a "${LOG_FILE}"
}

tmpfile="$(mktemp)"
trap 'rm -f "${tmpfile}"' EXIT

manifest_items="$(jq '.items | length' "${MANIFEST}")"
manifest_image_refs="$(jq '[.items[].images[]] | length' "${MANIFEST}")"
log "Starting image rename."
log "Manifest: ${MANIFEST} (items=${manifest_items}, image_refs=${manifest_image_refs}); source=${SOURCE_DIR}; target_root=${TARGET_ROOT}; log=${LOG_FILE}"

# Build a lookup from image filename -> {name, dir}.
jq -c '.items[] | {image: .images[], dir: .target_dir, name: .name_sanitized}' "${MANIFEST}" > "${tmpfile}"

declare -A IMAGE_TO_NAME
declare -A IMAGE_TO_DIR
declare -A MANIFEST_IMAGE_SEEN
duplicate_mappings=0

while IFS= read -r line; do
  image="$(printf '%s' "${line}" | jq -r '.image')"
  dir="$(printf '%s' "${line}" | jq -r '.dir')"
  name="$(printf '%s' "${line}" | jq -r '.name')"

  MANIFEST_IMAGE_SEEN["${image}"]=1

  if [[ -n "${IMAGE_TO_NAME[${image}]:-}" ]]; then
    ((duplicate_mappings++))
    log "WARN duplicate mapping for ${image}; keeping first entry (${IMAGE_TO_DIR[${image}]}) and ignoring ${dir}"
    continue
  fi

  IMAGE_TO_NAME["${image}"]="${name}"
  IMAGE_TO_DIR["${image}"]="${dir}"
done < "${tmpfile}"

log "Built lookup for ${#IMAGE_TO_NAME[@]} unique image filenames (${duplicate_mappings} duplicates ignored)."

shopt -s nullglob
moved=0
unmapped=0
seen=0
declare -A USED_IMAGE

for src in "${SOURCE_DIR}"/*; do
  [[ -f "${src}" ]] || continue
  ((seen++))

  filename="$(basename "${src}")"
  name="${IMAGE_TO_NAME[${filename}]:-}"
  dir="${IMAGE_TO_DIR[${filename}]:-}"

  if [[ -z "${name}" || -z "${dir}" ]]; then
    log "SKIP ${filename}: no mapping found in manifest"
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
    log "INFO collision for ${filename}: ${dest} exists; using ${TARGET_ROOT}/${dir}/${name}_${idx}.${ext}"
    dest="${TARGET_ROOT}/${dir}/${name}_${idx}.${ext}"
  fi

  mv "${src}" "${dest}"
  USED_IMAGE["${filename}"]=1
  log "MOVE ${filename} -> ${dest} (dir=${dir}, name=${name}, ext=${ext})"
  ((moved++))
done

manifest_missing=()
for image in "${!MANIFEST_IMAGE_SEEN[@]}"; do
  if [[ ! -e "${SOURCE_DIR}/${image}" && -z "${USED_IMAGE[${image}]:-}" ]]; then
    manifest_missing+=("${image}|${IMAGE_TO_DIR[${image}]}|${IMAGE_TO_NAME[${image}]}")
  fi
done

if [[ ${#manifest_missing[@]} -gt 0 ]]; then
  log "Manifest images not found in ${SOURCE_DIR} (${#manifest_missing[@]}):"
  for entry in "${manifest_missing[@]}"; do
    IFS='|' read -r img dir name <<< "${entry}"
    log "MISSING ${img} (target_dir=${dir}, name=${name})"
  done
fi

log "Completed. Scanned ${seen} files; moved ${moved}; ${unmapped} files had no mapping; manifest-only images missing from source: ${#manifest_missing[@]}."
