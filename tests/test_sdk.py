"""Verifies the public SDK configuration and reuse of Binance streaming."""

import pytest

from fast_orderbook.ingestion.binance import BinanceClient
from fast_orderbook.models import TradeEvent
from marketstream import MarketStream


def test_sdk_normalizes_symbols_and_events():
    stream = MarketStream(
        symbols=["btcusdt", " ETHUSDT ", "btcusdt"],
        events=["trade", "book_ticker"],
    )

    assert stream.symbols == ("BTCUSDT", "ETHUSDT")
    assert stream.events == ("trade", "bookTicker")


@pytest.mark.parametrize(
    ("symbols", "events"),
    [([], ["trade"]), (["BTCUSDT"], []), (["BTCUSDT"], ["invalid"])],
)
def test_sdk_rejects_invalid_configuration(symbols, events):
    with pytest.raises(ValueError):
        MarketStream(symbols=symbols, events=events)


@pytest.mark.asyncio
async def test_sdk_yields_existing_market_events(monkeypatch):
    expected = TradeEvent("binance", "BTCUSDT", 1, 10.0, 2.0, 3, 4, False)

    async def fake_stream(_client):
        yield expected

    monkeypatch.setattr(BinanceClient, "stream", fake_stream)
    stream = MarketStream("BTCUSDT", events=["trade"])

    assert await anext(stream) == expected
    await stream.aclose()
