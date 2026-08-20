#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_DEST_DIR="${1:-${ROOT_DIR}/data/benchmarks/source}"
GENERATED_DEST_DIR="${2:-${ROOT_DIR}/data/benchmarks/generated}"

BUCKET="us-west-2.opendata.source.coop"
PREFIX="developmentseed/stac-geoparquet"
ENDPOINT_URL="https://s3.us-west-2.amazonaws.com"
REGION="us-west-2"

mkdir -p "${SOURCE_DEST_DIR}" "${GENERATED_DEST_DIR}"

sync_prefix() {
    local name="$1"
    local dest_dir="${2:-${SOURCE_DEST_DIR}}"
    local source="s3://${BUCKET}/${PREFIX}/${name}/"
    local dest="${dest_dir}/${name}/"

    mkdir -p "${dest}"
    aws s3 sync "${source}" "${dest}" \
        --no-sign-request \
        --region "${REGION}" \
        --endpoint-url "${ENDPOINT_URL}" \
        --only-show-errors
}

sync_prefix "mspc-sentinel-2-l2a"
sync_prefix "mspc-sentinel-2-l2a-sorted"

"${ROOT_DIR}/scripts/generate-file-count-matched-geoparquet.sh" \
    "${SOURCE_DEST_DIR}/mspc-sentinel-2-l2a-sorted" \
    "${GENERATED_DEST_DIR}/mspc-sentinel-2-l2a-sorted-12-files"

find "${SOURCE_DEST_DIR}" "${GENERATED_DEST_DIR}" -name '*.parquet' -print | sort
