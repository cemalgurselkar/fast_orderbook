import argparse
import asyncio
import json
import resource
import struct
import time
from collections import defaultdict
from pathlib import Path
from uuid import uuid4

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer
from confluent_kafka.admin import (
    AdminClient,
    NewTopic,
)

from benchmarks.generator import (
    SyntheticMarketGenerator,
    TOPICS,
)
from benchmarks.metrics import BenchmarkMetrics


TOPIC_DATASETS = {
    "benchmark.trades": "trades",
    "benchmark.depth": "depth",
    "benchmark.book_ticker": "book_ticker",
    "benchmark.ticker": "ticker",
    "benchmark.klines": "klines",
}


class BenchmarkParquetWriter:

    def __init__(
        self,
        root: Path,
    ) -> None:
        self.root = root

    def write(
        self,
        topic: str,
        symbol: str,
        events: list[dict],
    ) -> int:
        if not events:
            return 0

        dataset = TOPIC_DATASETS[topic]

        output_dir = (
            self.root
            / dataset
            / symbol
        )

        output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        output = (
            output_dir
            / f"{uuid4().hex}.parquet"
        )

        table = pa.Table.from_pylist(events)

        pq.write_table(
            table,
            output,
            compression="zstd",
        )

        return len(events)


def ensure_topics(
    bootstrap_servers: str,
) -> None:
    admin = AdminClient(
        {
            "bootstrap.servers": bootstrap_servers,
        }
    )

    metadata = admin.list_topics(
        timeout=10
    )

    existing = set(metadata.topics)

    missing = [
        NewTopic(
            topic,
            num_partitions=8,
            replication_factor=1,
            config={
                "retention.ms": "600000",
            },
        )
        for topic in TOPICS
        if topic not in existing
    ]

    if not missing:
        return

    futures = admin.create_topics(missing)

    for topic, future in futures.items():
        future.result()
        print(f"Created topic: {topic}")


async def consume_and_write(
    metrics: BenchmarkMetrics,
    output_root: Path,
    bootstrap_servers: str,
    batch_size: int,
    queue_maxsize: int,
    stop_event: asyncio.Event,
) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"fast-orderbook-benchmark-{uuid4().hex}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "fetch.min.bytes": 1,
            "fetch.wait.max.ms": 50,
        }
    )

    consumer.subscribe(list(TOPICS))

    queue: asyncio.Queue[list[tuple[str, dict]] | None] = asyncio.Queue(
        maxsize=queue_maxsize
    )

    writer = BenchmarkParquetWriter(output_root)

    async def wait_for_assignment() -> None:
        expected_partitions = len(TOPICS) * 8
        deadline = time.monotonic() + 15.0

        while time.monotonic() < deadline:
            await asyncio.to_thread(consumer.poll, 0.1)

            assigned = consumer.assignment()

            if len(assigned) >= expected_partitions:
                print(
                    f"Consumer ready: "
                    f"{len(assigned)} partitions assigned"
                )
                return

        raise RuntimeError(
            "Kafka consumer assignment timeout. "
            f"Expected {expected_partitions} partitions, "
            f"got {len(consumer.assignment())}."
        )

    async def reader() -> None:
        idle_since = None

        while True:
            messages = await asyncio.to_thread(
                consumer.consume,
                5_000,
                0.1,
            )

            if not messages:
                if stop_event.is_set():
                    if idle_since is None:
                        idle_since = time.monotonic()
                    elif time.monotonic() - idle_since >= 2.0:
                        break

                continue

            idle_since = None
            batch: list[tuple[str, dict]] = []

            now_ns = time.perf_counter_ns()

            for message in messages:
                if message.error():
                    raise RuntimeError(message.error())

                headers = dict(message.headers() or [])
                sent_ns_raw = headers.get("sent_ns")

                if sent_ns_raw is not None:
                    sent_ns = struct.unpack(
                        "!Q",
                        sent_ns_raw,
                    )[0]

                    latency_ms = (
                        now_ns - sent_ns
                    ) / 1_000_000

                    metrics.latency_samples_ms.append(
                        latency_ms
                    )

                batch.append(
                    (
                        message.topic(),
                        orjson.loads(message.value()),
                    )
                )

            metrics.received += len(batch)

            await queue.put(batch)

            metrics.max_queue_depth = max(
                metrics.max_queue_depth,
                queue.qsize(),
            )

    async def disk_writer() -> None:
        buffers: dict[
            tuple[str, str],
            list[dict],
        ] = defaultdict(list)

        while True:
            kafka_batch = await queue.get()

            if kafka_batch is None:
                queue.task_done()
                break

            for topic, event in kafka_batch:
                key = (
                    topic,
                    event["symbol"],
                )

                buffer = buffers[key]
                buffer.append(event)

                if len(buffer) >= 10_000:
                    written = writer.write(
                        topic,
                        event["symbol"],
                        buffer,
                    )

                    metrics.written += written
                    buffers[key] = []

            queue.task_done()

        for (topic, symbol), buffer in buffers.items():
            if not buffer:
                continue

            written = writer.write(
                topic,
                symbol,
                buffer,
            )

            metrics.written += written

    try:
        await wait_for_assignment()

        reader_task = asyncio.create_task(
            reader()
        )

        writer_task = asyncio.create_task(
            disk_writer()
        )

        await reader_task

        await queue.join()
        await queue.put(None)

        await writer_task

    finally:
        consumer.close()

async def reader(
            consumer,
            queue,
            metrics,
            producer_stopped,):
    
    idle_since = None

    while True:
        messages = await asyncio.to_thread(
            consumer.consume,
            5_000,
            0.1,
        )

        if not messages:
            if producer_stopped.is_set():
                if idle_since is None:
                    idle_since = time.monotonic()

                elif time.monotonic() - idle_since >= 2.0:
                    break

            continue

        idle_since = None

        batch = []

        now_ns = time.perf_counter_ns()

        for message in messages:
            if message.error():
                raise RuntimeError(message.error())

            headers = dict(message.headers() or [])

            sent_ns_raw = headers.get("sent_ns")

            if sent_ns_raw is not None:
                sent_ns = struct.unpack("!Q", sent_ns_raw)[0]

                latency_ms = (now_ns - sent_ns) / 1_000_000

                metrics.latencies_ms.append(latency_ms)

            batch.append(
                (
                    message.topic(),
                    orjson.loads(message.value()),
                )
            )

        metrics.received += len(batch)

        await queue.put(batch)

        metrics.max_queue_depth = max(
            metrics.max_queue_depth,
            queue.qsize(),
        )

async def disk_writer(
    queue,
    writer,
    metrics,
    ):
    batches = defaultdict(list)

    while True:
        kafka_batch = await queue.get()

        if kafka_batch is None:
            queue.task_done()
            break

        for topic, event in kafka_batch:
            key = (topic, event["symbol"])

            buffer = batches[key]
            buffer.append(event)

            if len(buffer) >= 10_000:
                writer.write(
                    topic,
                    buffer,
                )

                metrics.written += len(buffer)

                batches[key] = []

        queue.task_done()

    for (topic, _symbol), buffer in batches.items():
        if not buffer:
            continue

        writer.write(
            topic,
            buffer,
        )

        metrics.written += len(buffer)


async def run_benchmark(
    rate: int,
    duration: float,
    bootstrap_servers: str,
    batch_size: int,
    queue_maxsize: int,
    report: Path,
) -> None:
    ensure_topics(
        bootstrap_servers
    )

    run_id = (
        f"{rate}_eps_"
        f"{int(time.time())}"
    )

    output_root = (
        Path("data/benchmark")
        / run_id
    )

    output_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    metrics = BenchmarkMetrics(
        target_rate=rate,
        duration_seconds=duration,
    )

    stop_event = asyncio.Event()

    consumer_task = asyncio.create_task(
        consume_and_write(
            metrics=metrics,
            output_root=output_root,
            bootstrap_servers=bootstrap_servers,
            batch_size=batch_size,
            queue_maxsize=queue_maxsize,
            stop_event=stop_event,
        )
    )

    await asyncio.sleep(2)

    generator = SyntheticMarketGenerator(
        bootstrap_servers=bootstrap_servers,
    )

    usage_before = resource.getrusage(
        resource.RUSAGE_SELF
    )

    wall_start = time.perf_counter()

    producer_result = await asyncio.to_thread(
        generator.run,
        rate,
        duration,
    )

    stop_event.set()

    await consumer_task

    wall_end = time.perf_counter()

    usage_after = resource.getrusage(
        resource.RUSAGE_SELF
    )

    metrics.wall_seconds = (
        wall_end
        - wall_start
    )

    metrics.produced = (
        producer_result["accepted"]
    )

    metrics.delivery_failed = (
        producer_result[
            "delivery_failed"
        ]
    )

    metrics.producer_queue_full = (
        producer_result[
            "queue_full_count"
        ]
    )

    metrics.cpu_seconds = (
        (
            usage_after.ru_utime
            + usage_after.ru_stime
        )
        -
        (
            usage_before.ru_utime
            + usage_before.ru_stime
        )
    )

    metrics.max_rss_mb = (
        usage_after.ru_maxrss
        / 1024
    )

    report.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with report.open(
        "a",
        encoding="utf-8",
    ) as handle:
        handle.write(
            metrics.markdown_row()
            + "\n"
        )

    raw_report = (
        report.parent
        / "benchmark_raw"
    )

    raw_report.mkdir(
        parents=True,
        exist_ok=True,
    )

    raw_data = {
        "target_rate": metrics.target_rate,
        "duration_seconds": duration,
        "produced": metrics.produced,
        "received": metrics.received,
        "written": metrics.written,
        "producer_rate": metrics.producer_rate,
        "throughput": metrics.throughput,
        "delivery_failed": metrics.delivery_failed,
        "producer_queue_full": (
            metrics.producer_queue_full
        ),
        "max_queue_depth": (
            metrics.max_queue_depth
        ),
        "latency_samples": len(
            metrics.latency_samples_ms
        ),
        "p50_ms": metrics.p50_ms,
        "p95_ms": metrics.p95_ms,
        "p99_ms": metrics.p99_ms,
        "cpu_percent": metrics.cpu_percent,
        "max_rss_mb": metrics.max_rss_mb,
        "wall_seconds": metrics.wall_seconds,
        "output_path": str(output_root),
    }

    (
        raw_report
        / f"{run_id}.json"
    ).write_text(
        json.dumps(
            raw_data,
            indent=2,
        ),
        encoding="utf-8",
    )

    print()
    print(
        f"Target:     {rate:,} events/s"
    )
    print(
        f"Produced:   {metrics.produced:,}"
    )
    print(
        f"Received:   {metrics.received:,}"
    )
    print(
        f"Written:    {metrics.written:,}"
    )
    print(
        f"Throughput: {metrics.throughput:,.0f} events/s"
    )
    print(
        f"P99:        {metrics.p99_ms:.3f} ms"
    )
    print(
        f"Max queue:  {metrics.max_queue_depth:,}"
    )


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--rate",
        type=int,
        required=True,
    )

    parser.add_argument(
        "--duration",
        type=float,
        default=15,
    )

    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
    )

    parser.add_argument(
        "--queue-maxsize",
        type=int,
        default=10000,
    )

    parser.add_argument(
        "--report",
        type=Path,
        required=True,
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    asyncio.run(
        run_benchmark(
            rate=args.rate,
            duration=args.duration,
            bootstrap_servers=args.bootstrap_servers,
            batch_size=args.batch_size,
            queue_maxsize=args.queue_maxsize,
            report=args.report,
        )
    )


if __name__ == "__main__":
    main()