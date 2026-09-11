"""Implements the async public SDK without platform-service dependencies."""

from collections.abc import AsyncIterator, Iterable
from typing import Self

from fast_orderbook.ingestion.binance import BinanceClient
from fast_orderbook.models import MarketEvent, TradeEvent

SUPPORTED_EVENTS = ("trade", "depth", "bookTicker", "ticker", "kline")
EVENT_NAMES = {
    "trade": "trade",
    "depth": "depth",
    "bookticker": "bookTicker",
    "ticker": "ticker",
    "kline": "kline",
}


def _normalize_symbols(symbols: str | Iterable[str]) -> tuple[str, ...]:
    values = (symbols,) if isinstance(symbols, str) else tuple(symbols)
    normalized = tuple(symbol.strip().upper() for symbol in values if symbol.strip())

    if not normalized:
        raise ValueError("At least one symbol is required.")

    return tuple(dict.fromkeys(normalized))


def _normalize_events(events: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []

    for event in events:
        key = event.strip().lower().replace("_", "")

        if key not in EVENT_NAMES:
            supported = ", ".join(SUPPORTED_EVENTS)
            raise ValueError(f"Unsupported event type: {event}. Supported values: {supported}")

        normalized.append(EVENT_NAMES[key])

    if not normalized:
        raise ValueError("At least one event type is required.")

    return tuple(dict.fromkeys(normalized))


class MarketStream:
    def __init__(
        self,
        symbols: str | Iterable[str],
        events: Iterable[str] = SUPPORTED_EVENTS,
    ) -> None:
        self.symbols = _normalize_symbols(symbols)
        self.events = _normalize_events(events)
        self._iterator: AsyncIterator[MarketEvent] | None = None

    def __aiter__(self) -> "MarketStream":
        return self

    async def __anext__(self) -> MarketEvent:
        if self._iterator is None:
            client = BinanceClient(
                symbols=tuple(symbol.lower() for symbol in self.symbols),
                event_types=self.events,
            )
            self._iterator = client.stream()

        return await anext(self._iterator)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, _exc_type, _exc, _traceback) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._iterator is not None:
            await self._iterator.aclose()
            self._iterator = None

    async def trades(self) -> AsyncIterator[TradeEvent]:
        client = BinanceClient(
            symbols=tuple(symbol.lower() for symbol in self.symbols),
            event_types=("trade",),
        )

        async for event in client.stream():
            if isinstance(event, TradeEvent):
                yield event
