from abc import ABC, abstractmethod
from fast_orderbook.models import TradeEvent

class ExchangeClient(ABC):
    @abstractmethod
    async def stream_trades(self):
        """Yield normalized trade events"""
        raise NotImplementedError