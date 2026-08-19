#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST_DIR="${1:-${ROOT_DIR}/data/benchmarks/source}"

BUCKET="us-west-2.opendata.source.coop"
PREFIX="developmentseed/stac-geoparquet"
ENDPOINT_URL="https://s3.us-west-2.amazonaws.com"
REGION="us-west-2"

mkdir -p "${DEST_DIR}"

sync_prefix() {
    local name="$1"
    local source="s3://${BUCKET}/${PREFIX}/${name}/"
    local dest="${DEST_DIR}/${name}/"

    mkdir -p "${dest}"
    aws s3 sync "${source}" "${dest}" \
        --no-sign-request \
        --region "${REGION}" \
        --endpoint-url "${ENDPOINT_URL}" \
        --only-show-errors
}

sync_prefix "mspc-sentinel-2-l2a"
sync_prefix "mspc-sentinel-2-l2a-sorted"

find "${DEST_DIR}" -name '*.parquet' -print | sort
