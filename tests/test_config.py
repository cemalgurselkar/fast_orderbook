from fast_orderbook.config import settings


def test_market_count():
    assert len(settings.symbols) == 10


def test_event_type_count():
    assert len(settings.event_types) == 5


def test_logical_stream_count():
    assert len(settings.symbols) * len(settings.event_types) == 50


def test_queue_is_bounded():
    assert settings.queue_maxsize > 0


def test_batch_size_is_positive():
    assert settings.batch_size > 0