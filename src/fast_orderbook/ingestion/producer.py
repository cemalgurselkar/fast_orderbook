import asyncio

from fast_orderbook.ingestion.binance import BinanceClient
from fast_orderbook.streaming.kafka import KafkaMarketProducer


async def main() -> None:
    client = BinanceClient()
    producer = KafkaMarketProducer()

    try:
        async for event in client.stream():
            producer.publish(event)
    finally:
        producer.close()


if __name__ == "__main__":
    asyncio.run(main())