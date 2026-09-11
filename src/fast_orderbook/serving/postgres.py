import os
from pathlib import Path

import duckdb
import psycopg


class PostgresGoldWriter:
    def __init__(
        self,
        dsn: str,
        gold_path: str,
    ) -> None:
        self.dsn = dsn
        self.gold_path = Path(gold_path)

    def create_table(self) -> None:
        query = """
        CREATE TABLE IF NOT EXISTS market_summary_1m (
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            window_start TIMESTAMP NOT NULL,
            trade_count BIGINT NOT NULL,
            total_quantity DOUBLE PRECISION NOT NULL,
            quote_volume DOUBLE PRECISION NOT NULL,
            min_price DOUBLE PRECISION NOT NULL,
            max_price DOUBLE PRECISION NOT NULL,
            vwap DOUBLE PRECISION NOT NULL,

            PRIMARY KEY (exchange, symbol, window_start)
        );
        """

        with psycopg.connect(self.dsn) as connection:
            connection.execute(query)

    def load_gold(self) -> int:
        rows = duckdb.sql(
            f"""
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
            FROM read_parquet('{self.gold_path}')
            """
        ).fetchall()

        if not rows:
            return 0

        query = """
        INSERT INTO market_summary_1m (
            exchange,
            symbol,
            window_start,
            trade_count,
            total_quantity,
            quote_volume,
            min_price,
            max_price,
            vwap
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)

        ON CONFLICT (exchange, symbol, window_start)
        DO UPDATE SET
            trade_count = EXCLUDED.trade_count,
            total_quantity = EXCLUDED.total_quantity,
            quote_volume = EXCLUDED.quote_volume,
            min_price = EXCLUDED.min_price,
            max_price = EXCLUDED.max_price,
            vwap = EXCLUDED.vwap;
        """

        with psycopg.connect(self.dsn) as connection, connection.cursor() as cursor:
            cursor.executemany(query, rows)

        return len(rows)


def main() -> None:
    writer = PostgresGoldWriter(
        dsn=os.getenv(
            "POSTGRES_DSN",
            "host=localhost port=5432 "
            "dbname=fast_orderbook "
            "user=fast_orderbook "
            "password=fast_orderbook_dev",
        ),
        gold_path="data/gold/market_summary_1m.parquet",
    )

    writer.create_table()
    loaded_rows = writer.load_gold()

    print(f"Loaded {loaded_rows} Gold rows into PostgreSQL.")


if __name__ == "__main__":
    main()