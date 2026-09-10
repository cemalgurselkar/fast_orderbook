from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq


TOPIC_DATASETS = {
    "market.trades": "trades",
    "market.depth": "depth",
    "market.book_ticker": "book_ticker",
    "market.ticker": "ticker",
    "market.klines": "klines",
}


class BronzeParquetWriter:
    def __init__(self, root_path: str = "data/bronze") -> None:
        self.root_path = Path(root_path)

    def write_batch(
        self,
        topic: str,
        events: list[dict],
    ) -> Path:
        if not events:
            raise ValueError("Cannot write an empty batch.")

        dataset = TOPIC_DATASETS[topic]
        symbol = events[0]["symbol"]

        now = datetime.now(timezone.utc)
        date = now.strftime("%Y-%m-%d")

        output_dir = (
            self.root_path
            / "binance"
            / dataset
            / symbol
            / date
        )

        output_dir.mkdir(parents=True, exist_ok=True)

        filename = (
            f"{now.strftime('%H-%M-%S-%f')}-"
            f"{uuid4().hex[:8]}.parquet"
        )

        output_path = output_dir / filename

        table = pa.Table.from_pylist(events)

        pq.write_table(
            table,
            output_path,
            compression="zstd",
        )

        return output_path