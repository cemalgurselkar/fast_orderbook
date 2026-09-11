# MarketStream V1 Finalization Plan

## Goal

MarketStream is a performance-oriented real-time market data platform
and Python SDK.

The project must support two primary use cases:

1. Data Platform
   - Binance WebSocket ingestion
   - Kafka streaming backbone
   - Bronze Parquet
   - PySpark Silver
   - DuckDB Gold
   - PostgreSQL serving
   - FastAPI REST API

2. Developer / Real-Time Streaming
   - pip-installable Python SDK
   - MarketStream public API
   - CLI
   - Kafka-backed live serving
   - FastAPI WebSocket endpoint
   - Minimal real-time dashboard

---

## Target Architecture

                         Binance WebSocket
                                |
                         Normalized Events
                                |
                              Kafka
                         /             \
                        /               \
             bronze-writer          live-serving
                    |                    |
              Bronze Parquet        FastAPI
                    |               /       \
                 PySpark           REST    WebSocket
                    |               |         |
                  Silver            └────┬────┘
                    |                    |
                  DuckDB             Streamlit
                    |                    |
                   Gold          [Refresh Live Data]
                    |
                PostgreSQL
                    |
                REST API


Python SDK is a separate lightweight interface:

Python User
    |
MarketStream
    |
Binance WebSocket
    |
Existing parsers/models
    |
MarketEvent

The SDK must NOT require Kafka, PostgreSQL, Spark, Airflow,
or Docker for basic live-stream usage.

---

## Existing System

Already implemented and working:

- Binance combined WebSocket ingestion
- 10 symbols
- 5 event types
  - trade
  - depth
  - bookTicker
  - ticker
  - kline
- normalized dataclass event models
- Kafka producer
- Kafka consumer
- 5 Kafka topics
- bounded async queue
- Bronze Parquet writer
- PySpark Bronze -> Silver
- DuckDB Silver -> Gold
- PostgreSQL Gold loader
- FastAPI REST serving
- Airflow batch orchestration
- Docker Compose infrastructure
- pytest suite
- synthetic benchmark generator
- py-spy profiling

Performance optimization already completed:

- Kafka batch consumption
- batch-based async queue
- Parquet batch size increased
- deterministic benchmark partition assignment

Measured 50K target benchmark:

Before optimization:
~12K effective events/s

After optimization:
~39K effective events/s

p99 latency:
~45 seconds -> ~1.05 seconds

Do not claim this is exact sustained system capacity.
Do not claim zero data loss.

---

# V1 Remaining Work

## 1. Public Python SDK

Create a clean public API.

Target usage:

    from marketstream import MarketStream

    stream = MarketStream(
        symbols=["BTCUSDT", "ETHUSDT"],
        events=["trade", "depth"],
    )

    async for event in stream:
        print(event)

Desired convenience API where reasonable:

    async for trade in MarketStream("BTCUSDT").trades():
        print(trade)

Requirements:

- reuse existing Binance WebSocket client
- reuse existing parsers
- reuse existing MarketEvent models
- do not duplicate ingestion logic
- Kafka must not be required
- clean shutdown
- sensible validation
- async-first design

---

## 2. CLI

Target:

    marketstream stream \
        --symbols BTCUSDT ETHUSDT \
        --events trade depth

CLI should use the public MarketStream SDK rather than implement
another ingestion pipeline.

Keep CLI small.

---

## 3. Kafka Live Serving

Add a separate Kafka consumer group for real-time serving.

Consumer groups:

    bronze-writer
    live-serving

They must consume independently.

Live serving must not interfere with Bronze persistence.

---

## 4. FastAPI WebSocket

Add real-time WebSocket serving.

Example endpoint:

    /ws/markets/{symbol}

Flow:

    Kafka
      ->
    live-serving consumer
      ->
    FastAPI WebSocket
      ->
    client

Support live events such as:

- trades
- depth
- book ticker
- ticker
- klines

Do not route live updates through PostgreSQL.

PostgreSQL remains the serving database for historical/aggregated
Gold queries.

---

## 5. Streamlit Live Dashboard

A Streamlit dashboard already exists in the project.

Do NOT replace it with React, Vue, or another frontend framework.
Extend the existing Streamlit UI.

The dashboard is a lightweight demonstration interface for the
real-time market-data platform, not a separate frontend project.

### Dashboard Requirements

Display useful market information such as:

- selected symbol
- current / latest price
- best bid
- best ask
- recent trades
- small order-book view where practical
- timestamp of the latest received data

### Manual Refresh

Add a clearly visible refresh button to the existing Streamlit UI.

Example:

    [ Refresh Live Data ]

When the user clicks the button:

1. Fetch the latest available market data.
2. Update the displayed values.
3. Show the newest available timestamp/data.
4. Refresh only through the existing serving architecture rather than
   creating a duplicate Binance ingestion implementation inside the UI.

The Streamlit application should remain a presentation/client layer.

Preferred data flow:

    Binance
       |
      Kafka
       |
    live-serving
       |
    FastAPI
       |
    Streamlit
       |
      User

For manual refresh, the Streamlit UI may request the latest state from
the FastAPI serving layer.

Do NOT make Streamlit directly responsible for:

- Binance WebSocket ingestion
- Kafka consumption
- event normalization
- Bronze/Silver/Gold processing

Those responsibilities belong to their existing architectural layers.

### Real-Time vs Manual Refresh

The architecture should support real-time streaming through the
FastAPI WebSocket endpoint.

However, the existing Streamlit dashboard should also provide a
manual refresh button so the user can explicitly request and display
the latest available market state.

Keep this implementation simple.

Do not introduce a large frontend architecture solely to make the
dashboard real-time.

## 6. Packaging

The project should be installable as a Python package.

Target:

    pip install marketstream

Local development must support:

    pip install -e .

Expose:

    from marketstream import MarketStream

and CLI:

    marketstream stream ...

Keep heavy platform dependencies separated where practical so
basic SDK usage does not unnecessarily require Spark/Airflow/etc.

---

## 7. Tests

Existing tests must continue passing.

Add focused tests for:

- MarketStream API
- CLI argument handling
- live event routing
- WebSocket serving where practical

Do not replace working tests unnecessarily.

---

## 8. Documentation

README should include:

- project purpose
- architecture diagram
- quick start
- Python SDK example
- CLI example
- full platform setup
- REST API example
- WebSocket example
- benchmark methodology
- benchmark results
- limitations

Clearly distinguish:

    SDK mode

from:

    Full data-platform mode

---

# Engineering Constraints

Do not rewrite working components without a concrete reason.

Prefer simple, modular Python.

Use OOP where it represents real responsibilities.

Avoid unnecessary abstraction layers.

Avoid aggressive try/except.

Expected state should generally use explicit checks.

Unexpected errors should surface.

Preserve the existing event models and parsing logic where possible.

Preserve benchmark reproducibility.

Do not introduce new infrastructure only for resume keywords.

Do not add C++ in V1.

Do not add Azure in V1.

Do not add Redis unless the architecture actually requires it.

Do not turn the project into a frontend project.

Do not claim 1M events/s.

Do not claim zero data loss.

---

# Implementation Order

1. Inspect existing repository
2. Public MarketStream SDK
3. SDK tests
4. CLI
5. CLI tests
6. Kafka live-serving consumer
7. FastAPI WebSocket
8. minimal dashboard
9. packaging cleanup
10. full test suite
11. benchmark regression check
12. README / architecture documentation
13. repository cleanup
14. V1 release candidate

After every major milestone:

- run relevant tests
- preserve existing behavior
- report files changed
- report architectural decisions
- do not silently redesign unrelated components