import asyncio
import sys

from src.storage.redis.client import redis_manager
from src.utils.logger import setup_logger

from src.executor_v2.stream_keys import StreamKeys
from src.executor_v2.state import State
from src.executor_v2.exchange import Exchange

class Executor:
    def __init__(self):
        self.redis = redis_manager.connect
        self.logger = setup_logger(
            name="executor_message",
            log_file="logs/executor/executor_binance.log"
        )

        self.group_name:str = "executor_01"
        self.data_types:list[str] = ["orderbook","trades","market_price","open_interect"]
        self.streamings:dict[str,str] = {}

        self.stream_keys = StreamKeys()
        self.state = State()
        self.exchange = Exchange(self.logger)

    async def fetch_account_data(self):
        pass

    async def refresh_stream_keys(self):
        while True:
            self.streamings = await self.stream_keys.get_stream_keys(
                group_name=self.group_name,
                data_types=self.data_types,
                streamings=self.streamings,
                logger=self.logger
            )
            await asyncio.sleep(60)

    async def consume_data(self):
        

    async def run(self):
        await self.redis.sadd("registry:streams:orderbook","md:binance:spot:BTC-USDT:orderbook")
        
        tasks = []

        tasks.append(asyncio.create_task(self.refresh_stream_keys()))
        tasks.append(asyncio.create_task(self.consume_data()))

        await asyncio.gather(*tasks)

if __name__ == "__main__":
    obj = Executor()
    asyncio.run(obj.run())