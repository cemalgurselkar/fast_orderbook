"""Serves historical PostgreSQL summaries and Kafka-backed live market data."""

import asyncio
import os
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from psycopg.rows import dict_row

from fast_orderbook.serving.live import KafkaLiveService

DEFAULT_POSTGRES_DSN = (
    "host=localhost port=5432 dbname=fast_orderbook user=fast_orderbook password=fast_orderbook_dev"
)


def create_app(
    live_service: KafkaLiveService | None = None,
    live_enabled: bool | None = None,
    postgres_dsn: str | None = None,
) -> FastAPI:
    service = live_service or KafkaLiveService(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    enabled = live_enabled

    if enabled is None:
        enabled = os.getenv("MARKETSTREAM_LIVE_ENABLED", "true").lower() == "true"

    dsn = postgres_dsn or os.getenv("POSTGRES_DSN", DEFAULT_POSTGRES_DSN)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        if enabled:
            service.start()

        yield

        if enabled:
            await service.stop()

    application = FastAPI(
        title="MarketStream API",
        version="0.1.0",
        lifespan=lifespan,
    )
    application.state.live_service = service

    @application.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/markets/{symbol}/summary")
    def market_summary(
        symbol: str,
        limit: int = Query(default=60, ge=1, le=1000),
    ) -> list[dict]:
        query = """
            SELECT
                exchange,
                symbol,
                window_start,
                trade_count,
                total_quantity,
                quote_volume,
                min_price,
                max_price,
                vwap
            FROM market_summary_1m
            WHERE symbol = %s
            ORDER BY window_start DESC
            LIMIT %s;
        """

        with psycopg.connect(dsn, row_factory=dict_row) as connection:
            return connection.execute(query, (symbol.upper(), limit)).fetchall()

    @application.get("/markets/{symbol}/live")
    def live_market(symbol: str) -> dict:
        return service.hub.snapshot(symbol)

    @application.websocket("/ws/markets/{symbol}")
    async def live_market_websocket(websocket: WebSocket, symbol: str) -> None:
        normalized_symbol = symbol.upper()
        queue = service.hub.subscribe(normalized_symbol)
        await websocket.accept()

        try:
            snapshot = service.hub.snapshot(normalized_symbol)

            if snapshot["latest_event_at_ms"] is not None:
                await websocket.send_json({"event_type": "snapshot", **snapshot})

            while True:
                event_task = asyncio.create_task(queue.get())
                client_task = asyncio.create_task(websocket.receive())
                completed, pending = await asyncio.wait(
                    (event_task, client_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for task in pending:
                    task.cancel()

                await asyncio.gather(*pending, return_exceptions=True)

                if client_task in completed:
                    client_message = client_task.result()

                    if (
                        client_message["type"] == "websocket.disconnect"
                        or client_message.get("text") == "close"
                    ):
                        break
                    continue

                await websocket.send_json(event_task.result())
        except WebSocketDisconnect:
            pass
        finally:
            service.hub.unsubscribe(normalized_symbol, queue)

    return application


app = create_app()
