from abc import ABC,abstractmethod
import asyncio


class StreamBase(ABC):
    def __init__(self,exchange_id:str,mkt_type:str):
        self.exchange_id:str = exchange_id
        self.mkt_type:str = mkt_type
        self.queue = asyncio.Queue(maxsize=5000)
        self.ws = None
        self.redis = None

    @abstractmethod
    async def connect(self):
        pass