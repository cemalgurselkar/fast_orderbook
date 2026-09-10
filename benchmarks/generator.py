import struct
import time

import orjson
from confluent_kafka import Producer


SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "LTCUSDT",
)


TOPICS = (
    "benchmark.trades",
    "benchmark.depth",
    "benchmark.book_ticker",
    "benchmark.ticker",
    "benchmark.klines",
)


class SyntheticMarketGenerator:

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        sample_every: int = 100,
    ) -> None:
        self.sample_every = sample_every

        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "queue.buffering.max.messages": 1_000_000,
                "queue.buffering.max.kbytes": 1_048_576,
                "linger.ms": 5,
                "batch.num.messages": 10_000,
                "compression.type": "lz4",
                "acks": 1,
            }
        )

        self.templates = self._build_templates()

        self.accepted = 0
        self.delivery_failed = 0
        self.queue_full_count = 0

    @staticmethod
    def _build_templates() -> list[
        tuple[str, str, bytes]
    ]:
        templates = []

        for symbol in SYMBOLS:
            templates.extend(
                (
                    (
                        "benchmark.trades",
                        symbol,
                        orjson.dumps(
                            {
                                "exchange": "benchmark",
                                "symbol": symbol,
                                "trade_id": 1,
                                "price": 50000.0,
                                "quantity": 0.01,
                                "event_time_ms": 1,
                                "trade_time_ms": 1,
                                "is_buyer_maker": False,
                            }
                        ),
                    ),
                    (
                        "benchmark.depth",
                        symbol,
                        orjson.dumps(
                            {
                                "exchange": "benchmark",
                                "symbol": symbol,
                                "event_time_ms": 1,
                                "first_update_id": 1,
                                "final_update_id": 2,
                                "bids": [
                                    [50000.0, 1.0],
                                    [49999.0, 2.0],
                                ],
                                "asks": [
                                    [50001.0, 1.0],
                                    [50002.0, 2.0],
                                ],
                            }
                        ),
                    ),
                    (
                        "benchmark.book_ticker",
                        symbol,
                        orjson.dumps(
                            {
                                "exchange": "benchmark",
                                "symbol": symbol,
                                "update_id": 1,
                                "best_bid_price": 50000.0,
                                "best_bid_quantity": 1.0,
                                "best_ask_price": 50001.0,
                                "best_ask_quantity": 1.0,
                            }
                        ),
                    ),
                    (
                        "benchmark.ticker",
                        symbol,
                        orjson.dumps(
                            {
                                "exchange": "benchmark",
                                "symbol": symbol,
                                "event_time_ms": 1,
                                "last_price": 50000.0,
                                "price_change": 100.0,
                                "price_change_percent": 0.2,
                                "weighted_avg_price": 49950.0,
                                "base_volume": 1000.0,
                                "quote_volume": 50_000_000.0,
                            }
                        ),
                    ),
                    (
                        "benchmark.klines",
                        symbol,
                        orjson.dumps(
                            {
                                "exchange": "benchmark",
                                "symbol": symbol,
                                "event_time_ms": 1,
                                "open_time_ms": 1,
                                "close_time_ms": 60000,
                                "interval": "1m",
                                "open_price": 50000.0,
                                "high_price": 50100.0,
                                "low_price": 49900.0,
                                "close_price": 50050.0,
                                "volume": 100.0,
                                "trade_count": 1000,
                                "is_closed": False,
                            }
                        ),
                    ),
                )
            )

        return templates

    def _delivery_callback(
        self,
        error,
        _message,
    ) -> None:
        if error is not None:
            self.delivery_failed += 1

    def run(
        self,
        rate: int,
        duration: float,
    ) -> dict:
        start_ns = time.perf_counter_ns()

        duration_ns = int(
            duration * 1_000_000_000
        )

        index = 0

        while True:
            now_ns = time.perf_counter_ns()
            elapsed_ns = now_ns - start_ns

            if elapsed_ns >= duration_ns:
                break

            expected = int(
                elapsed_ns
                * rate
                / 1_000_000_000
            )

            backlog = expected - self.accepted

            if backlog <= 0:
                time.sleep(0.0001)
                continue

            burst = min(
                backlog,
                10_000,
            )

            for _ in range(burst):
                topic, symbol, payload = (
                    self.templates[
                        index % len(self.templates)
                    ]
                )

                headers = None

                if (
                    self.accepted
                    % self.sample_every
                    == 0
                ):
                    headers = [
                        (
                            "sent_ns",
                            struct.pack(
                                "!Q",
                                time.perf_counter_ns(),
                            ),
                        )
                    ]

                while True:
                    try:
                        self.producer.produce(
                            topic=topic,
                            key=symbol.encode(),
                            value=payload,
                            headers=headers,
                            callback=self._delivery_callback,
                        )
                        break

                    except BufferError:
                        self.queue_full_count += 1
                        self.producer.poll(0.001)

                self.accepted += 1
                index += 1

            self.producer.poll(0)

        undelivered = self.producer.flush(30)

        return {
            "accepted": self.accepted,
            "delivery_failed": (
                self.delivery_failed
                + undelivered
            ),
            "queue_full_count": (
                self.queue_full_count
            ),
        }