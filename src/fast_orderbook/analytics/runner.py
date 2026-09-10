from fast_orderbook.analytics.duckdb import DuckDBAnalytics
from fast_orderbook.config import settings


def main() -> None:
    analytics = DuckDBAnalytics(
        db_path=settings.duckdb_path,
        bronze_path=settings.bronze_path,
        silver_path=settings.silver_path,
        gold_path=settings.gold_path,
    )

    try:
        gold_output = analytics.create_gold()

        print(
            f"Gold dataset created: {gold_output}"
        )

    finally:
        analytics.close()


if __name__ == "__main__":
    main()