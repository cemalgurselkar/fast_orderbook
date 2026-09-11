from abc import ABC, abstractmethod


class ExchangeClient(ABC):
    @abstractmethod
    async def stream_trades(self):
        """Yield normalized trade events"""
        raise NotImplementedError