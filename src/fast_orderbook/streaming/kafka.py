import asyncio
from dataclasses import asdict

import orjson
from confluent_kafka import Consumer, Producer

from fast_orderbook.models import (
    BookTickerEvent,
    DepthEvent,
    KlineEvent,
    MarketEvent,
    TickerEvent,
    TradeEvent,
)

TOPICS = {
    TradeEvent: "market.trades",
    DepthEvent: "market.depth",
    BookTickerEvent: "market.book_ticker",
    TickerEvent: "market.ticker",
    KlineEvent: "market.klines",
}

MARKET_TOPICS = tuple(TOPICS.values())


class KafkaMarketProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092") -> None:
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "linger.ms": 5,
                "batch.num.messages": 10_000,
            }
        )

    def publish(self, event: MarketEvent) -> None:
        topic = TOPICS[type(event)]

        self.producer.produce(
            topic=topic,
            key=event.symbol.encode(),
            value=orjson.dumps(asdict(event)),
        )

        self.producer.poll(0)

    def close(self) -> None:
        self.producer.flush()


class KafkaMarketConsumer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "fast-orderbook-bronze",
        consume_batch_size: int = 5_000,
    ) -> None:
        self.consume_batch_size = consume_batch_size

        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
                "fetch.min.bytes": 1,
                "fetch.wait.max.ms": 50,
            }
        )

        self.consumer.subscribe(list(MARKET_TOPICS))

    async def consume_batches(self):
        while True:
            messages = await asyncio.to_thread(
                self.consumer.consume,
                self.consume_batch_size,
                0.1,
            )

            if not messages:
                continue

            batch: list[tuple[str, dict]] = []

            for message in messages:
                if message.error():
                    raise RuntimeError(message.error())

                batch.append(
                    (
                        message.topic(),
                        orjson.loads(message.value()),
                    )
                )

            yield batch

    async def consume(self):
        async for batch in self.consume_batches():
            for item in batch:
                yield item

    def close(self) -> None:
        self.consumer.close()