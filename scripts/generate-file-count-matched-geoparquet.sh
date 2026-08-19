#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

INPUT_DIR="${1:-${ROOT_DIR}/data/benchmarks/source/mspc-sentinel-2-l2a-sorted}"
OUTPUT_DIR="${2:-${ROOT_DIR}/data/benchmarks/generated/mspc-sentinel-2-l2a-sorted-12-files}"
TARGET_FILES="${3:-12}"
SORT_EXPR="${4:-\"hash:hash\"}"

if ! command -v duckdb >/dev/null 2>&1; then
    echo "duckdb CLI is required" >&2
    exit 1
fi

rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

mapfile -t INPUT_FILES < <(find "${INPUT_DIR}" -name '*.parquet' | sort)

if [[ "${#INPUT_FILES[@]}" -eq 0 ]]; then
    echo "no parquet files found under ${INPUT_DIR}" >&2
    exit 1
fi

sql_quote() {
    local value="$1"
    value="${value//\'/\'\'}"
    printf "'%s'" "${value}"
}

sql_list() {
    local start="$1"
    local end="$2"
    local sep=""

    printf "["
    for ((j = start; j < end; j++)); do
        printf "%s" "${sep}"
        sql_quote "${INPUT_FILES[j]}"
        sep=", "
    done
    printf "]"
}

total_files="${#INPUT_FILES[@]}"
for ((i = 0; i < TARGET_FILES; i++)); do
    start=$((i * total_files / TARGET_FILES))
    end=$(((i + 1) * total_files / TARGET_FILES))
    part="$(printf "%03d" "${i}")"
    files="$(sql_list "${start}" "${end}")"
    output_file="$(sql_quote "${OUTPUT_DIR}/part-${part}.parquet")"

    duckdb -c "
COPY (
    SELECT *
    FROM read_parquet(${files}, hive_partitioning = false)
    ORDER BY ${SORT_EXPR}
) TO ${output_file} (
    FORMAT parquet,
    COMPRESSION zstd
);
"
done

actual_files="$(find "${OUTPUT_DIR}" -maxdepth 1 -name '*.parquet' | wc -l | tr -d ' ')"
if [[ "${actual_files}" != "${TARGET_FILES}" ]]; then
    echo "expected ${TARGET_FILES} output files, found ${actual_files}" >&2
    exit 1
fi

find "${OUTPUT_DIR}" -name '*.parquet' -print | sort
