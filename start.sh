#!/usr/bin/env bash

set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

REPORT_DIR="$ROOT_DIR/reports"
TIMESTAMP="$(date '+%Y-%m-%d_%H-%M-%S')"
REPORT="$REPORT_DIR/test_report_$TIMESTAMP.md"

mkdir -p "$REPORT_DIR"

exec > >(tee -a "$REPORT") 2>&1


section() {
    echo
    echo "============================================================"
    echo "$1"
    echo "============================================================"
    echo
}


section "FAST-ORDERBOOK TEST REPORT"

echo "Generated: $(date --iso-8601=seconds)"
echo "Host: $(hostname)"
echo "Kernel: $(uname -sr)"
echo "Python: $(python3 --version)"
echo


section "SYSTEM"

echo "CPU:"
lscpu | grep -E \
    'Model name|CPU\(s\)|Thread|Core|Socket' || true

echo
echo "Memory:"
free -h

echo
echo "Disk:"
df -h "$ROOT_DIR"


section "PROJECT DATA"

python3 - <<'PY'
from pathlib import Path
import pyarrow.parquet as pq


def rows(path: Path) -> tuple[int, int]:
    files = list(path.rglob("*.parquet"))

    total = sum(
        pq.ParquetFile(file).metadata.num_rows
        for file in files
    )

    return len(files), total


print("BRONZE")

bronze = Path("data/bronze/binance")

for dataset in (
    "trades",
    "depth",
    "book_ticker",
    "ticker",
    "klines",
):
    file_count, row_count = rows(
        bronze / dataset
    )

    print(
        f"{dataset:<12} "
        f"files={file_count:<5} "
        f"rows={row_count}"
    )


print("\nSILVER")

silver = Path("data/silver")

for dataset in (
    "trades",
    "depth",
    "book_ticker",
    "ticker",
    "klines",
):
    file_count, row_count = rows(
        silver / dataset
    )

    print(
        f"{dataset:<12} "
        f"files={file_count:<5} "
        f"rows={row_count}"
    )


print("\nGOLD")

gold = Path(
    "data/gold/market_summary_1m.parquet"
)

if gold.exists():
    metadata = pq.ParquetFile(gold).metadata
    print(f"market_summary_1m rows={metadata.num_rows}")
else:
    print("market_summary_1m MISSING")
PY


section "PYTEST"

python3 -m pytest \
    tests \
    -v \
    --tb=short

PYTEST_EXIT=$?


section "GOLD QUALITY"

python3 - <<'PY'
from pathlib import Path
import duckdb

path = Path(
    "data/gold/market_summary_1m.parquet"
)

if not path.exists():
    print("Gold dataset missing.")
    raise SystemExit(1)

con = duckdb.connect()

row = con.execute(
    """
    SELECT
        COUNT(*) AS rows,
        COUNT(DISTINCT symbol) AS symbols,
        SUM(trade_count) AS trades,
        MIN(window_start) AS first_window,
        MAX(window_start) AS last_window
    FROM read_parquet(?)
    """,
    [str(path)],
).fetchone()

print(f"Gold rows:       {row[0]}")
print(f"Symbols:         {row[1]}")
print(f"Aggregated trades: {row[2]}")
print(f"First window:    {row[3]}")
print(f"Last window:     {row[4]}")

con.close()
PY

GOLD_EXIT=$?


section "DOCKER SERVICES"

if command -v docker >/dev/null 2>&1; then
    docker compose ps || true
else
    echo "Docker not available."
fi


section "RESULT"

if [[ $PYTEST_EXIT -eq 0 && $GOLD_EXIT -eq 0 ]]; then
    echo "STATUS: PASS"
    FINAL_EXIT=0
else
    echo "STATUS: FAIL"
    echo "pytest_exit=$PYTEST_EXIT"
    echo "gold_exit=$GOLD_EXIT"
    FINAL_EXIT=1
fi

echo
echo "Report:"
echo "$REPORT"

exit "$FINAL_EXIT"