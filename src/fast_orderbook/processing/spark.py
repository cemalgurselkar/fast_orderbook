from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from fast_orderbook.config import settings


class SilverProcessor:
    def __init__(
        self,
        bronze_path: str = settings.bronze_path,
        silver_path: str = settings.silver_path,
    ) -> None:
        self.bronze_path = Path(bronze_path) / "binance"
        self.silver_path = Path(silver_path)

        self.spark = (
            SparkSession.builder
            .appName("fast-orderbook-silver")
            .master("local[*]")
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("WARN")

    def _read(self, dataset: str) -> DataFrame:
        return (
            self.spark.read
            .option("recursiveFileLookup", "true")
            .parquet(str(self.bronze_path / dataset))
        )

    @staticmethod
    def _base_validation(df: DataFrame) -> DataFrame:
        return df.filter(
            F.col("exchange").isNotNull()
            & F.col("symbol").isNotNull()
        )

    @staticmethod
    def _keep_latest(
        df: DataFrame,
        keys: list[str],
        order_column: str,
    ) -> DataFrame:
        window = (
            Window
            .partitionBy(*keys)
            .orderBy(F.col(order_column).desc())
        )

        return (
            df.withColumn("_row_number", F.row_number().over(window))
            .filter(F.col("_row_number") == 1)
            .drop("_row_number")
        )

    def process_trades(self) -> DataFrame:
        df = self._base_validation(self._read("trades"))

        df = df.filter(
            (F.col("trade_id").isNotNull())
            & (F.col("price") > 0)
            & (F.col("quantity") > 0)
            & (F.col("event_time_ms") > 0)
            & (F.col("trade_time_ms") > 0)
        )

        return self._keep_latest(
            df,
            ["exchange", "symbol", "trade_id"],
            "event_time_ms",
        )

    def process_depth(self) -> DataFrame:
        df = self._base_validation(self._read("depth"))

        df = df.filter(
            (F.col("event_time_ms") > 0)
            & (F.col("first_update_id") >= 0)
            & (F.col("final_update_id") >= F.col("first_update_id"))
        )

        return self._keep_latest(
            df,
            ["exchange", "symbol", "final_update_id"],
            "event_time_ms",
        )

    def process_book_ticker(self) -> DataFrame:
        df = self._base_validation(self._read("book_ticker"))

        df = df.filter(
            (F.col("update_id") >= 0)
            & (F.col("best_bid_price") > 0)
            & (F.col("best_ask_price") > 0)
            & (F.col("best_bid_quantity") >= 0)
            & (F.col("best_ask_quantity") >= 0)
        )

        return df.dropDuplicates(
            ["exchange", "symbol", "update_id"]
        )

    def process_ticker(self) -> DataFrame:
        df = self._base_validation(self._read("ticker"))

        df = df.filter(
            (F.col("event_time_ms") > 0)
            & (F.col("last_price") > 0)
            & (F.col("base_volume") >= 0)
            & (F.col("quote_volume") >= 0)
        )

        return self._keep_latest(
            df,
            ["exchange", "symbol", "event_time_ms"],
            "event_time_ms",
        )

    def process_klines(self) -> DataFrame:
        df = self._base_validation(self._read("klines"))

        df = df.filter(
            (F.col("event_time_ms") > 0)
            & (F.col("open_time_ms") > 0)
            & (F.col("close_time_ms") >= F.col("open_time_ms"))
            & (F.col("open_price") > 0)
            & (F.col("high_price") > 0)
            & (F.col("low_price") > 0)
            & (F.col("close_price") > 0)
            & (F.col("volume") >= 0)
            & (F.col("trade_count") >= 0)
        )

        # Aynı 1m candle Binance tarafından kapanana kadar
        # tekrar tekrar gönderilebilir. Son halini tutuyoruz.
        return self._keep_latest(
            df,
            ["exchange", "symbol", "interval", "open_time_ms"],
            "event_time_ms",
        )

    def write(self, dataset: str, df: DataFrame) -> None:
        output = self.silver_path / dataset

        (
            df.write
            .mode("overwrite")
            .parquet(str(output))
        )

    def run(self) -> None:
        processors = {
            "trades": self.process_trades,
            "depth": self.process_depth,
            "book_ticker": self.process_book_ticker,
            "ticker": self.process_ticker,
            "klines": self.process_klines,
        }

        for dataset, processor in processors.items():
            df = processor()
            self.write(dataset, df)

            print(
                f"Silver {dataset:<12} "
                f"rows={df.count()}"
            )

    def close(self) -> None:
        self.spark.stop()


def main() -> None:
    processor = SilverProcessor()

    try:
        processor.run()
    finally:
        processor.close()


if __name__ == "__main__":
    main()