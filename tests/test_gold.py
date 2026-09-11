from pathlib import Path

import duckdb

GOLD_PATH = Path(
    "data/gold/market_summary_1m.parquet"
)


def test_gold_exists():
    assert GOLD_PATH.exists()


def test_gold_has_rows():
    count = duckdb.execute(
        """
        SELECT COUNT(*)
        FROM read_parquet(?)
        """,
        [str(GOLD_PATH)],
    ).fetchone()[0]

    assert count > 0


def test_gold_contains_expected_symbols():
    symbols = {
        row[0]
        for row in duckdb.execute(
            """
            SELECT DISTINCT symbol
            FROM read_parquet(?)
            """,
            [str(GOLD_PATH)],
        ).fetchall()
    }

    assert "BTCUSDT" in symbols
    assert "ETHUSDT" in symbols


def test_gold_metrics_are_valid():
    invalid_rows = duckdb.execute(
        """
        SELECT COUNT(*)
        FROM read_parquet(?)
        WHERE
            trade_count <= 0
            OR total_quantity <= 0
            OR quote_volume <= 0
            OR min_price <= 0
            OR max_price <= 0
            OR vwap <= 0
            OR min_price > max_price
        """,
        [str(GOLD_PATH)],
    ).fetchone()[0]

    assert invalid_rows == 0