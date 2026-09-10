import asyncio
from collections.abc import AsyncIterator

import orjson
import websockets

from fast_orderbook.config import settings
from fast_orderbook.models import (
    BookTickerEvent,
    DepthEvent,
    KlineEvent,
    MarketEvent,
    TickerEvent,
    TradeEvent,
)


class BinanceClient:
    BASE_URL = "wss://stream.binance.com:9443/stream?streams="

    def __init__(self) -> None:
        self.url = self._build_stream_url()

    @staticmethod
    def _build_stream_url() -> str:
        streams: list[str] = []

        for symbol in settings.symbols:
            streams.extend(
                (
                    f"{symbol}@trade",
                    f"{symbol}@depth@100ms",
                    f"{symbol}@bookTicker",
                    f"{symbol}@ticker",
                    f"{symbol}@kline_1m",
                )
            )

        return BinanceClient.BASE_URL + "/".join(streams)

    async def stream(self) -> AsyncIterator[MarketEvent]:
        retry_delay = 1

        while True:
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=1024,
                ) as websocket:
                    print("Connected to Binance: 50 logical streams")
                    retry_delay = 1

                    async for raw_message in websocket:
                        message = orjson.loads(raw_message)

                        yield self._parse(
                            message["stream"],
                            message["data"],
                        )

            except asyncio.CancelledError:
                raise

            except (
                websockets.ConnectionClosed,
                OSError,
                TimeoutError,
            ) as error:
                print(
                    f"Binance connection lost: {error}. "
                    f"Retrying in {retry_delay}s..."
                )

                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)

    @staticmethod
    def _parse(
        stream_name: str,
        data: dict,
    ) -> MarketEvent:
        stream = stream_name.lower()

        if "@trade" in stream:
            return TradeEvent(
                exchange="binance",
                symbol=data["s"],
                trade_id=data["t"],
                price=float(data["p"]),
                quantity=float(data["q"]),
                event_time_ms=data["E"],
                trade_time_ms=data["T"],
                is_buyer_maker=data["m"],
            )

        if "@depth" in stream:
            return DepthEvent(
                exchange="binance",
                symbol=data["s"],
                event_time_ms=data["E"],
                first_update_id=data["U"],
                final_update_id=data["u"],
                bids=tuple(
                    (float(price), float(quantity))
                    for price, quantity in data["b"]
                ),
                asks=tuple(
                    (float(price), float(quantity))
                    for price, quantity in data["a"]
                ),
            )

        if "@bookticker" in stream:
            return BookTickerEvent(
                exchange="binance",
                symbol=data["s"],
                update_id=data["u"],
                best_bid_price=float(data["b"]),
                best_bid_quantity=float(data["B"]),
                best_ask_price=float(data["a"]),
                best_ask_quantity=float(data["A"]),
            )

        if "@ticker" in stream:
            return TickerEvent(
                exchange="binance",
                symbol=data["s"],
                event_time_ms=data["E"],
                last_price=float(data["c"]),
                price_change=float(data["p"]),
                price_change_percent=float(data["P"]),
                weighted_avg_price=float(data["w"]),
                base_volume=float(data["v"]),
                quote_volume=float(data["q"]),
            )

        if "@kline" in stream:
            kline = data["k"]

            return KlineEvent(
                exchange="binance",
                symbol=data["s"],
                event_time_ms=data["E"],
                open_time_ms=kline["t"],
                close_time_ms=kline["T"],
                interval=kline["i"],
                open_price=float(kline["o"]),
                high_price=float(kline["h"]),
                low_price=float(kline["l"]),
                close_price=float(kline["c"]),
                volume=float(kline["v"]),
                trade_count=kline["n"],
                is_closed=kline["x"],
            )

        raise ValueError(
            f"Unsupported Binance stream: {stream_name}"
        )