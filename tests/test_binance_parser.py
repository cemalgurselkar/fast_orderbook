from fast_orderbook.ingestion.binance import BinanceClient
from fast_orderbook.models import (
    BookTickerEvent,
    DepthEvent,
    KlineEvent,
    TickerEvent,
    TradeEvent,
)


def test_parse_trade():
    data = {
        "e": "trade",
        "E": 1000,
        "s": "BTCUSDT",
        "t": 123,
        "p": "50000.50",
        "q": "0.01",
        "T": 1001,
        "m": True,
    }

    event = BinanceClient._parse("btcusdt@trade", data)

    assert isinstance(event, TradeEvent)
    assert event.symbol == "BTCUSDT"
    assert event.trade_id == 123
    assert event.price == 50000.50
    assert event.quantity == 0.01
    assert event.is_buyer_maker is True


def test_parse_depth():
    data = {
        "e": "depthUpdate",
        "E": 2000,
        "s": "ETHUSDT",
        "U": 10,
        "u": 12,
        "b": [["2400.0", "1.5"]],
        "a": [["2401.0", "2.0"]],
    }

    event = BinanceClient._parse("ethusdt@depth@100ms", data)

    assert isinstance(event, DepthEvent)
    assert event.symbol == "ETHUSDT"
    assert event.first_update_id == 10
    assert event.final_update_id == 12
    assert event.bids == ((2400.0, 1.5),)
    assert event.asks == ((2401.0, 2.0),)


def test_parse_book_ticker():
    data = {
        "u": 99,
        "s": "SOLUSDT",
        "b": "100.0",
        "B": "5.0",
        "a": "100.1",
        "A": "6.0",
    }

    event = BinanceClient._parse("solusdt@bookTicker", data)

    assert isinstance(event, BookTickerEvent)
    assert event.symbol == "SOLUSDT"
    assert event.update_id == 99
    assert event.best_bid_price == 100.0
    assert event.best_ask_price == 100.1


def test_parse_ticker():
    data = {
        "e": "24hrTicker",
        "E": 3000,
        "s": "BNBUSDT",
        "c": "700.0",
        "p": "10.0",
        "P": "1.45",
        "w": "695.0",
        "v": "1000.0",
        "q": "695000.0",
    }

    event = BinanceClient._parse("bnbusdt@ticker", data)

    assert isinstance(event, TickerEvent)
    assert event.symbol == "BNBUSDT"
    assert event.last_price == 700.0
    assert event.price_change == 10.0
    assert event.base_volume == 1000.0


def test_parse_kline():
    data = {
        "e": "kline",
        "E": 4000,
        "s": "XRPUSDT",
        "k": {
            "t": 1000,
            "T": 59999,
            "i": "1m",
            "o": "1.30",
            "h": "1.40",
            "l": "1.20",
            "c": "1.35",
            "v": "10000",
            "n": 250,
            "x": True,
        },
    }

    event = BinanceClient._parse("xrpusdt@kline_1m", data)

    assert isinstance(event, KlineEvent)
    assert event.symbol == "XRPUSDT"
    assert event.interval == "1m"
    assert event.close_price == 1.35
    assert event.trade_count == 250
    assert event.is_closed is True