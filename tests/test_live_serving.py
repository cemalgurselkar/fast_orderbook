"""Verifies independent live routing, snapshots, and WebSocket delivery."""

import pytest

from fast_orderbook.serving.api import create_app
from fast_orderbook.serving.live import KafkaLiveService, LiveMarketHub


def trade_event(price: float = 50_000.0) -> dict:
    return {
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "trade_id": 1,
        "price": price,
        "quantity": 0.1,
        "event_time_ms": 1,
        "trade_time_ms": 1,
        "is_buyer_maker": False,
    }


def test_live_hub_routes_events_and_builds_snapshot():
    hub = LiveMarketHub()
    queue = hub.subscribe("BTCUSDT")

    payload = hub.publish("market.trades", trade_event())
    snapshot = hub.snapshot("BTCUSDT")

    assert queue.get_nowait() == payload
    assert snapshot["latest_price"] == 50_000.0
    assert snapshot["recent_trades"][0]["trade_id"] == 1


def test_live_consumer_group_is_independent():
    service = KafkaLiveService()

    assert service.group_id == "marketstream-live-serving"
    assert service.group_id != "marketstream-bronze-writer"


class FakeWebSocket:
    def __init__(self) -> None:
        self.messages: list[dict] = []
        self.accepted = False

    async def accept(self) -> None:
        self.accepted = True

    async def send_json(self, message: dict) -> None:
        self.messages.append(message)

    async def receive(self) -> dict:
        return {"type": "websocket.disconnect"}


@pytest.mark.asyncio
async def test_websocket_sends_current_snapshot():
    service = KafkaLiveService()
    service.hub.publish("market.trades", trade_event())
    application = create_app(live_service=service, live_enabled=False)
    route = next(route for route in application.routes if route.path == "/ws/markets/{symbol}")
    websocket = FakeWebSocket()

    await route.endpoint(websocket, "BTCUSDT")

    assert websocket.accepted is True
    assert websocket.messages[0]["event_type"] == "snapshot"
    assert websocket.messages[0]["latest_price"] == 50_000.0


def test_live_rest_endpoint_returns_latest_state():
    service = KafkaLiveService()
    service.hub.publish(
        "market.book_ticker",
        {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "update_id": 1,
            "best_bid_price": 49_999.0,
            "best_bid_quantity": 1.0,
            "best_ask_price": 50_001.0,
            "best_ask_quantity": 1.0,
        },
    )
    application = create_app(live_service=service, live_enabled=False)
    route = next(route for route in application.routes if route.path == "/markets/{symbol}/live")

    response = route.endpoint("BTCUSDT")

    assert response["best_bid"] == 49_999.0
    assert response["best_ask"] == 50_001.0
