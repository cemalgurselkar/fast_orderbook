from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class TradeEvent:
    exchange: str
    symbol: str
    trade_id: int
    price: float
    quantity: float
    event_time_ms: int
    trade_time_ms: int
    is_buyer_maker: bool

@dataclass(slots=True, frozen=True)
class DepthEvent:
    exchange: str
    symbol: str
    event_time_ms: int
    first_update_id: int
    final_update_id: int
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]


@dataclass(slots=True, frozen=True)
class BookTickerEvent:
    exchange: str
    symbol: str
    update_id: int
    best_bid_price: float
    best_bid_quantity: float
    best_ask_price: float
    best_ask_quantity: float


@dataclass(slots=True, frozen=True)
class TickerEvent:
    exchange: str
    symbol: str
    event_time_ms: int
    last_price: float
    price_change: float
    price_change_percent: float
    weighted_avg_price: float
    base_volume: float
    quote_volume: float


@dataclass(slots=True, frozen=True)
class KlineEvent:
    exchange: str
    symbol: str
    event_time_ms: int
    open_time_ms: int
    close_time_ms: int
    interval: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    trade_count: int
    is_closed: bool


MarketEvent = (
    TradeEvent
    | DepthEvent
    | BookTickerEvent
    | TickerEvent
    | KlineEvent
)