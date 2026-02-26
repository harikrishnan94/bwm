#!/bin/sh
set -eu

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <path-to-bwm>" >&2
  exit 2
fi

BWM_BIN="$1"
if [ ! -x "$BWM_BIN" ]; then
  echo "bwm binary is not executable: $BWM_BIN" >&2
  exit 1
fi

WORK_DIR="${TMPDIR:-/tmp}/bwm_tcp_e2e_validation_$$"
OUT_DIR="$WORK_DIR/out"
JSON_PATH="$WORK_DIR/out.json"

cleanup() {
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT INT TERM

mkdir -p "$WORK_DIR"

metric_mean() {
  key="$1"
  awk -v key="$key" '
    $0 ~ ("\"" key "\"[[:space:]]*:[[:space:]]*\\{") {
      if (match($0, /"mean":[[:space:]]*[-0-9.eE+]+/)) {
        m = substr($0, RSTART, RLENGTH)
        sub(/"mean":[[:space:]]*/, "", m)
        print m
        exit
      }
    }
  ' "$JSON_PATH"
}

"$BWM_BIN" \
  --medium tcp \
  --mode all \
  --algo lz4 \
  --duration-sec 2 \
  --repeats 1 \
  --threads 2 \
  --chunk-size 4096 \
  --queue-depth 64 \
  --port 9321 \
  --seed 17 \
  --output-dir "$OUT_DIR" \
  --json "$JSON_PATH" \
  >/dev/null

if [ ! -f "$JSON_PATH" ]; then
  echo "expected JSON output file missing" >&2
  exit 1
fi

grep -q '"W_eff_raw_GBps"' "$JSON_PATH" || {
  echo "missing W_eff_raw_GBps metric" >&2
  exit 1
}
grep -q '"R_eff_raw_GBps"' "$JSON_PATH" || {
  echo "missing R_eff_raw_GBps metric" >&2
  exit 1
}
grep -q '"M_read"' "$JSON_PATH" || {
  echo "missing M_read metric" >&2
  exit 1
}

raw_write_mean="$(metric_mean W_eff_raw_GBps)"
raw_read_mean="$(metric_mean R_eff_raw_GBps)"
m_read_mean="$(metric_mean M_read)"

[ -n "$raw_write_mean" ] || {
  echo "missing W_eff_raw_GBps.mean value" >&2
  exit 1
}
[ -n "$raw_read_mean" ] || {
  echo "missing R_eff_raw_GBps.mean value" >&2
  exit 1
}
[ -n "$m_read_mean" ] || {
  echo "missing M_read.mean value" >&2
  exit 1
}

awk "BEGIN { exit !($raw_write_mean > 0.0) }" || {
  echo "expected W_eff_raw_GBps.mean > 0, got $raw_write_mean" >&2
  exit 1
}
awk "BEGIN { exit !($raw_read_mean > 0.0) }" || {
  echo "expected R_eff_raw_GBps.mean > 0, got $raw_read_mean" >&2
  exit 1
}
awk "BEGIN { exit !($m_read_mean > 0.0) }" || {
  echo "expected M_read.mean > 0, got $m_read_mean" >&2
  exit 1
}

exit 0
