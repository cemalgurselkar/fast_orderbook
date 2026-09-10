#!/usr/bin/env bash

set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TIMESTAMP="$(date '+%Y-%m-%d_%H-%M-%S')"
REPORT_DIR="$ROOT_DIR/reports"

REPORT="$REPORT_DIR/profile_report_$TIMESTAMP.md"
RAW_PROFILE="$REPORT_DIR/profile_raw_$TIMESTAMP.txt"
SPEEDSCOPE="$REPORT_DIR/profile_$TIMESTAMP.json"

RATE="${RATE:-50000}"
DURATION="${DURATION:-15}"

mkdir -p "$REPORT_DIR"

echo "============================================================"
echo "FAST-ORDERBOOK CPU PROFILE"
echo "============================================================"
echo
echo "Rate:     $RATE events/sec"
echo "Duration: ${DURATION}s"
echo

echo "Running py-spy..."

py-spy record \
    --format speedscope \
    --output "$SPEEDSCOPE" \
    -- \
    python3 -m benchmarks.runner \
        --rate "$RATE" \
        --duration "$DURATION" \
        --report /tmp/fast_orderbook_profile_benchmark.md

PROFILE_EXIT=$?

echo
echo "Collecting function-level CPU profile..."

py-spy record \
    --format raw \
    --output "$RAW_PROFILE" \
    -- \
    python3 -m benchmarks.runner \
        --rate "$RATE" \
        --duration "$DURATION" \
        --report /tmp/fast_orderbook_profile_benchmark_raw.md

RAW_EXIT=$?

{
    echo "# Fast-Orderbook Profiling Report"
    echo
    echo "Generated: $(date --iso-8601=seconds)"
    echo
    echo "## Environment"
    echo
    echo "- CPU: $(lscpu | awk -F: '/Model name/ {gsub(/^ +/, "", $2); print $2}')"
    echo "- Logical CPUs: $(nproc)"
    echo "- Memory: $(free -h | awk '/Mem:/ {print $2}')"
    echo "- Python: $(python3 --version 2>&1)"
    echo "- Target rate: ${RATE} events/sec"
    echo "- Duration: ${DURATION}s"
    echo
    echo "## Profile Artifacts"
    echo
    echo "- Speedscope: \`$(basename "$SPEEDSCOPE")\`"
    echo "- Raw profile: \`$(basename "$RAW_PROFILE")\`"
    echo
    echo "## Exit Status"
    echo
    echo "- Speedscope profile: $PROFILE_EXIT"
    echo "- Raw profile: $RAW_EXIT"
    echo
    echo "## Raw CPU Samples"
    echo
    echo '```text'

    if [[ -f "$RAW_PROFILE" ]]; then
        sort -t' ' -k2 -nr "$RAW_PROFILE" | head -n 80
    else
        echo "Raw profile was not generated."
    fi

    echo '```'
} > "$REPORT"

echo
echo "============================================================"
echo "PROFILE COMPLETE"
echo "============================================================"
echo
echo "Report:"
echo "$REPORT"
echo
echo "Raw profile:"
echo "$RAW_PROFILE"
echo
echo "Speedscope:"
echo "$SPEEDSCOPE"