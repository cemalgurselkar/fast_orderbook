from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    symbols: tuple[str, ...] = (
        "btcusdt",
        "ethusdt",
        "solusdt",
        "bnbusdt",
        "xrpusdt",
        "dogeusdt",
        "adausdt",
        "avaxusdt",
        "linkusdt",
        "ltcusdt",
    )

    event_types: tuple[str, ...] = (
        "trade",
        "depth",
        "bookTicker",
        "ticker",
        "kline",
    )

    queue_maxsize: int = 10_000
    batch_size: int = 1_000
    batch_timeout_seconds: float = 2.0

    bronze_path: str = "data/bronze"
    silver_path: str = "data/silver"
    gold_path: str = "data/gold"
    duckdb_path: str = "data/fast_orderbook.duckdb"


settings = Settings()