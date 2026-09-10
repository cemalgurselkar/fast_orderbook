import asyncio
import time
from collections import defaultdict

from fast_orderbook.config import settings
from fast_orderbook.storage.parquet import BronzeParquetWriter
from fast_orderbook.streaming.kafka import KafkaMarketConsumer

KafkaBatch = list[tuple[str, dict]]
QueueItem = KafkaBatch | None

PARQUET_BATCH_SIZE = 10_000


async def consume_kafka(
    consumer: KafkaMarketConsumer,
    queue: asyncio.Queue,
) -> None:
    async for batch in consumer.consume_batches():
        await queue.put(batch)


async def write_bronze(
    queue: asyncio.Queue,
    writer: BronzeParquetWriter,
) -> None:
    buffers: dict[tuple[str, str], list[dict]] = defaultdict(list)
    last_flush: dict[tuple[str, str], float] = {}

    try:
        while True:
            try:
                kafka_batch = await asyncio.wait_for(
                    queue.get(),
                    timeout=0.1,
                )
            except asyncio.TimeoutError:
                kafka_batch = ...

            now = time.monotonic()

            if kafka_batch is None:
                break

            if kafka_batch is not ...:
                for topic, event in kafka_batch:
                    key = (topic, event["symbol"])
                    buffer = buffers[key]

                    if not buffer:
                        last_flush[key] = now

                    buffer.append(event)

                    if len(buffer) >= PARQUET_BATCH_SIZE:
                        writer.write_batch(topic, buffer)
                        buffers[key] = []
                        last_flush[key] = now

                queue.task_done()

            for key, buffer in list(buffers.items()):
                if not buffer:
                    continue

                elapsed = now - last_flush.get(key, now)

                if elapsed >= settings.batch_timeout_seconds:
                    topic, _symbol = key

                    writer.write_batch(topic, buffer)

                    buffers[key] = []
                    last_flush[key] = now

    finally:
        for (topic, _symbol), buffer in buffers.items():
            if buffer:
                writer.write_batch(topic, buffer)


async def main() -> None:
    queue: asyncio.Queue[QueueItem] = asyncio.Queue(
        maxsize=settings.queue_maxsize
    )

    consumer = KafkaMarketConsumer(
        consume_batch_size=5_000,
    )

    writer = BronzeParquetWriter(
        settings.bronze_path,
    )

    consumer_task = asyncio.create_task(
        consume_kafka(
            consumer,
            queue,
        )
    )

    writer_task = asyncio.create_task(
        write_bronze(
            queue,
            writer,
        )
    )

    try:
        await consumer_task

    except asyncio.CancelledError:
        consumer_task.cancel()

        await asyncio.gather(
            consumer_task,
            return_exceptions=True,
        )

        await queue.join()
        await queue.put(None)

        await writer_task

        raise

    finally:
        consumer.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bronze consumer stopped.")