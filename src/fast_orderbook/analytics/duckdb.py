from pathlib import Path

import duckdb


class DuckDBAnalytics:

    def __init__(
        self,
        db_path: str,
        bronze_path: str,
        silver_path: str,
        gold_path: str,
    ) -> None:
        self.db_path = Path(db_path)
        self.bronze_path = Path(bronze_path)
        self.silver_path = Path(silver_path)
        self.gold_path = Path(gold_path)

        self.db_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.gold_path.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.connection = duckdb.connect(
            database=str(self.db_path)
        )

        self.sql_dir = Path(__file__).parent / "sql"

    def _load_sql(
        self,
        filename: str,
        **kwargs,
    ) -> str:
        sql_path = self.sql_dir / filename

        return sql_path.read_text(
            encoding="utf-8"
        ).format(**kwargs)

    def create_gold(self) -> Path:
        silver_glob = (
            self.silver_path
            / "trades"
            / "*.parquet"
        )

        output_path = (
            self.gold_path
            / "market_summary_1m.parquet"
        )

        if output_path.exists():
            output_path.unlink()

        query = self._load_sql(
            "create_gold.sql",
            silver_glob=silver_glob.as_posix(),
            gold_output=output_path.as_posix(),
        )

        self.connection.execute(query)

        return output_path

    def close(self) -> None:
        self.connection.close()