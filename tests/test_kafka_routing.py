import pytest

from fast_orderbook.models import (
    BookTickerEvent,
    DepthEvent,
    KlineEvent,
    TickerEvent,
    TradeEvent,
)
from fast_orderbook.streaming.kafka import TOPICS


@pytest.mark.parametrize(
    ("event_type", "expected_topic"),
    [
        (TradeEvent, "market.trades"),
        (DepthEvent, "market.depth"),
        (BookTickerEvent, "market.book_ticker"),
        (TickerEvent, "market.ticker"),
        (KlineEvent, "market.klines"),
    ],
)
def test_event_topic_routing(event_type, expected_topic):
    assert TOPICS[event_type] == expected_topic


def test_exactly_five_market_topics():
    assert len(TOPICS) == 5
    assert len(set(TOPICS.values())) == 5