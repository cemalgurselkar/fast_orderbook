from datetime import datetime

import psycopg
from fastapi import FastAPI, Query
from psycopg.rows import dict_row


app = FastAPI(
    title="fast-orderbook API",
    version="0.1.0",
)

POSTGRES_DSN = (
    "host=localhost "
    "port=5432 "
    "dbname=fast_orderbook "
    "user=fast_orderbook "
    "password=fast_orderbook_dev"
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/markets/{symbol}/summary")
def market_summary(
    symbol: str,
    limit: int = Query(default=60, ge=1, le=1000),
) -> list[dict]:
    query = """
        SELECT
            exchange,
            symbol,
            window_start,
            trade_count,
            total_quantity,
            quote_volume,
            min_price,
            max_price,
            vwap
        FROM market_summary_1m
        WHERE symbol = %s
        ORDER BY window_start DESC
        LIMIT %s;
    """

    with psycopg.connect(
        POSTGRES_DSN,
        row_factory=dict_row,
    ) as connection:
        rows = connection.execute(
            query,
            (symbol.upper(), limit),
        ).fetchall()

    return rows