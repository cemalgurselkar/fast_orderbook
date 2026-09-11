# MarketStream

MarketStream is an async-first Python SDK and a real-time market-data platform. It can stream
normalized Binance events directly in a Python application, or run a complete Kafka-backed data
pipeline with Bronze, Silver, Gold, REST, WebSocket, and Streamlit serving layers.

## Architecture

```text
Binance WebSocket
        |
 Normalized Events
        |
      Kafka
        |-- bronze-writer -> Bronze -> PySpark -> Silver -> DuckDB -> Gold -> PostgreSQL
        |                                                                     |
        |                                                               FastAPI REST
        |
        `-- live-serving --------------------------------------> FastAPI live REST/WebSocket
                                                                              |
                                                                     Streamlit and clients
```

The public SDK uses the existing Binance client, parsers, and event models directly. It does not
require Kafka, PostgreSQL, Spark, Airflow, or Docker.

## SDK mode

Install the local development package:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Stream selected event types:

```python
import asyncio

from marketstream import MarketStream


async def main():
    stream = MarketStream(
        symbols=["BTCUSDT", "ETHUSDT"],
        events=["trade", "depth"],
    )

    async with stream:
        async for event in stream:
            print(event)


asyncio.run(main())
```

Trade-only convenience API:

```python
import asyncio

from marketstream import MarketStream


async def main():
    async for trade in MarketStream("BTCUSDT").trades():
        print(trade)


asyncio.run(main())
```

Supported event types are `trade`, `depth`, `bookTicker`, `ticker`, and `kline`.

## CLI

The CLI uses the public SDK:

```bash
marketstream stream \
    --symbols BTCUSDT ETHUSDT \
    --events trade depth
```

The same command can be run without the installed console script:

```bash
python -m marketstream stream --symbols BTCUSDT --events trade
```

## Full platform mode

Requirements:

- Python 3.11
- OpenJDK 17
- Docker with the Docker Compose plugin
- curl

Start the complete development platform:

```bash
./setup.sh
```

The setup script installs the `platform`, `api`, and `ui` extras and starts:

- Kafka at `localhost:9092`
- PostgreSQL at `localhost:5432`
- Airflow at `http://localhost:8080`
- FastAPI at `http://localhost:8000`
- Streamlit at `http://localhost:8501`
- Binance producer and Bronze writer processes

Local process logs and PID files are stored under `.runtime/`.

Individual dependency groups can also be installed explicitly:

```bash
pip install -e ".[platform]"
pip install -e ".[api]"
pip install -e ".[ui]"
pip install -e ".[dev]"
```

## Data layers

- Bronze stores normalized Kafka events in partitioned Parquet files.
- Silver uses PySpark for validation and deduplication.
- Gold uses DuckDB for one-minute market summaries and VWAP calculations.
- PostgreSQL serves historical Gold summaries to the REST API.
- The `marketstream-live-serving` Kafka consumer group independently feeds live API state.

The Bronze writer and live-serving groups consume independently. Live traffic does not pass
through PostgreSQL and does not interfere with Bronze persistence.

## REST API

Start the API separately:

```bash
pip install -e ".[api]"
uvicorn fast_orderbook.serving.api:app --reload
```

Health:

```bash
curl http://localhost:8000/health
```

Historical one-minute summaries:

```bash
curl "http://localhost:8000/markets/BTCUSDT/summary?limit=60"
```

Latest in-memory market state:

```bash
curl http://localhost:8000/markets/BTCUSDT/live
```

`POSTGRES_DSN`, `KAFKA_BOOTSTRAP_SERVERS`, `MARKETSTREAM_LIVE_ENABLED`, and
`MARKETSTREAM_API_URL` can be provided as environment variables.

## WebSocket API

Live normalized events are available at:

```text
ws://localhost:8000/ws/markets/BTCUSDT
```

Python example:

```python
import asyncio
import json

import websockets


async def main():
    async with websockets.connect("ws://localhost:8000/ws/markets/BTCUSDT") as socket:
        async for message in socket:
            print(json.loads(message))


asyncio.run(main())
```

The endpoint can emit trades, depth, book ticker, ticker, and kline events. Slow clients receive
the newest bounded live data rather than an unbounded in-memory backlog.

## Streamlit dashboard

Start the UI separately:

```bash
pip install -e ".[ui]"
streamlit run src/fast_orderbook/ui/ui.py
```

The dashboard displays the selected symbol, latest price, best bid and ask, recent trades, a small
order-book view, latest event time, and historical Gold charts. `Refresh Live Data` explicitly
requests the latest state from FastAPI. Automatic refresh can also be enabled.

The UI remains a client layer. It does not connect to Binance or consume Kafka directly.

## Tests

```bash
pip install -e ".[dev]"
pytest
ruff check .
```

The suite covers Binance parsing, public SDK validation and iteration, CLI arguments, Kafka topic
routing, Parquet output, Gold quality, live state routing, and WebSocket delivery.

## Benchmark

Start Kafka before running the synthetic benchmark:

```bash
./benchmarks/run.sh
```

The benchmark records requested and achieved producer rates, received and written counts,
producer-to-consumer latency samples, CPU time, memory, queue depth, and Parquet throughput.

On the development laptop, the 50K target improved from roughly 12K to 39,294 effective written
events per second, while sampled p99 latency improved from roughly 45 seconds to 1.46 seconds.
These measurements do not establish exact sustained capacity or zero data loss.
Produced, received, and written counts must be compared when interpreting a run.

## Limitations

- Binance is currently the only exchange implementation.
- The development Kafka and PostgreSQL configuration is not production hardened.
- Bronze consumer offsets currently use automatic commits.
- Silver processing rewrites datasets rather than using incremental partitions.
- Live state is in-memory, assumes one API worker, and resets when that process restarts.
- The Streamlit UI polls the latest REST snapshot; external clients can use the WebSocket endpoint.
- Benchmark results depend on the local machine, payload distribution, storage, and Kafka settings.
- MarketStream does not claim one million sustained events per second or zero data loss.
