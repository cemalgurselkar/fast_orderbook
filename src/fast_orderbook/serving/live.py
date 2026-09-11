"""Routes a dedicated Kafka consumer group into live snapshots and subscribers."""

import asyncio
import time
from collections import defaultdict, deque

from fast_orderbook.streaming.kafka import KafkaMarketConsumer

TOPIC_EVENT_TYPES = {
    "market.trades": "trade",
    "market.depth": "depth",
    "market.book_ticker": "bookTicker",
    "market.ticker": "ticker",
    "market.klines": "kline",
}


class LiveMarketHub:
    def __init__(self, queue_size: int = 1_000, recent_trade_limit: int = 50) -> None:
        self.queue_size = queue_size
        self._subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self._state: dict[str, dict] = defaultdict(dict)
        self._recent_trades: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=recent_trade_limit)
        )

    def subscribe(self, symbol: str) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=self.queue_size)
        self._subscribers[symbol.upper()].add(queue)
        return queue

    def unsubscribe(self, symbol: str, queue: asyncio.Queue) -> None:
        subscribers = self._subscribers.get(symbol.upper())

        if subscribers is None:
            return

        subscribers.discard(queue)

        if not subscribers:
            self._subscribers.pop(symbol.upper(), None)

    def publish(self, topic: str, event: dict) -> dict:
        event_type = TOPIC_EVENT_TYPES[topic]
        symbol = event["symbol"].upper()
        payload = {
            "event_type": event_type,
            "received_at_ms": int(time.time() * 1_000),
            **event,
        }

        self._update_state(symbol, event_type, payload)

        for queue in tuple(self._subscribers.get(symbol, ())):
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(payload)

        return payload

    def snapshot(self, symbol: str) -> dict:
        normalized_symbol = symbol.upper()
        state = self._state.get(normalized_symbol, {})
        book_ticker = state.get("bookTicker", {})
        depth = state.get("depth", {})
        latest = state.get("latest", {})

        return {
            "symbol": normalized_symbol,
            "latest_price": state.get("latest_price"),
            "best_bid": book_ticker.get("best_bid_price"),
            "best_ask": book_ticker.get("best_ask_price"),
            "bids": depth.get("bids", [])[:10],
            "asks": depth.get("asks", [])[:10],
            "recent_trades": list(self._recent_trades.get(normalized_symbol, ())),
            "latest_event_type": latest.get("event_type"),
            "latest_event_at_ms": latest.get("received_at_ms"),
        }

    def _update_state(self, symbol: str, event_type: str, payload: dict) -> None:
        state = self._state[symbol]
        state[event_type] = payload
        state["latest"] = payload

        if event_type == "trade":
            self._recent_trades[symbol].appendleft(payload)
            state["latest_price"] = payload["price"]
        elif event_type == "ticker":
            state["latest_price"] = payload["last_price"]
        elif event_type == "kline":
            state["latest_price"] = payload["close_price"]


class KafkaLiveService:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "marketstream-live-serving",
        hub: LiveMarketHub | None = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.hub = hub or LiveMarketHub()
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._consume())

    async def stop(self) -> None:
        if self._task is None:
            return

        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        self._task = None

    async def _consume(self) -> None:
        consumer = KafkaMarketConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
        )

        try:
            async for topic, event in consumer.consume():
                self.hub.publish(topic, event)
        finally:
            consumer.close()
