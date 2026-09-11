#!/usr/bin/env bash
# Runs the MarketStream benchmark with the configured 10K Parquet batch size.

set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

REPORT_DIR="$ROOT_DIR/reports"
TIMESTAMP="$(date '+%Y-%m-%d_%H-%M-%S')"

REPORT="$REPORT_DIR/benchmark_report_$TIMESTAMP.md"

DURATION="${DURATION:-15}"

RATES=(
    50000
    100000
    250000
    500000
    750000
    1000000
)

mkdir -p "$REPORT_DIR"

cat > "$REPORT" <<EOF
# MarketStream Benchmark Report

Generated: $(date --iso-8601=seconds)

## Environment

- CPU: $(lscpu | awk -F: '/Model name/ {gsub(/^ +/, "", $2); print $2}')
- Logical CPUs: $(nproc)
- Memory: $(free -h | awk '/Mem:/ {print $2}')
- Kernel: $(uname -r)
- Python: $(python3 --version 2>&1)
- Benchmark duration per workload: ${DURATION}s
- Kafka partitions per benchmark topic: 8
- Consumer queue capacity: 10,000
- Parquet batch size: 10,000
- Parquet compression: ZSTD
- Latency sampling: 1 / 100 events

## Results

| Target eps | Producer eps | Written eps | Produced | Received | Written | Failed | p50 ms | p95 ms | p99 ms | CPU | Max RSS MB | Max Queue | Producer Queue Full |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
EOF

echo
echo "============================================================"
echo "MARKETSTREAM BENCHMARK"
echo "============================================================"
echo
echo "Duration per workload: ${DURATION}s"
echo "Report: $REPORT"
echo

for RATE in "${RATES[@]}"; do

    echo
    echo "------------------------------------------------------------"
    echo "TARGET: ${RATE} events/sec"
    echo "------------------------------------------------------------"
    echo

    python3 -m benchmarks.runner \
        --rate "$RATE" \
        --duration "$DURATION" \
        --report "$REPORT"

    EXIT_CODE=$?

    if [[ $EXIT_CODE -ne 0 ]]; then
        echo
        echo "Benchmark failed at ${RATE} events/sec."
        echo "Exit code: $EXIT_CODE"

        cat >> "$REPORT" <<EOF

## Failure

Benchmark process failed at target rate:

\`${RATE} events/sec\`

Exit code:

\`${EXIT_CODE}\`
EOF

        exit "$EXIT_CODE"
    fi

    sleep 2

done


cat >> "$REPORT" <<EOF

## Interpretation

- **Target eps**: requested synthetic input rate.
- **Producer eps**: rate the Python load generator actually submitted.
- **Written eps**: events durably written to benchmark Parquet output divided by measured wall time.
- **Produced / Received / Written** should be compared for loss/backlog analysis.
- **p50/p95/p99** are sampled producer-to-consumer latencies.
- **Max Queue** shows pressure on the bounded in-process queue.
- **Producer Queue Full** shows pressure on librdkafka's producer buffer.

A target workload must not be described as sustained throughput unless the measured producer and writer rates support that claim.

EOF

echo
echo "============================================================"
echo "BENCHMARK COMPLETE"
echo "============================================================"
echo
echo "$REPORT"
