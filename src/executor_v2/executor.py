import asyncio
import json

from src.storage.redis.client import redis_manager
from src.utils.logger import setup_logger

from src.strategies.taker_trend_strategy_v2 import TakerTrendStrategy

from src.executor_v2.stream_keys import StreamKeys
from src.executor_v2.data_manager import DataManager
from src.executor_v2.state import State
from src.executor_v2.exchange import Exchange
from src.executor_v2.risk import Risk

class Executor:
    def __init__(self):
        self.redis = redis_manager.connect
        self.logger = setup_logger(
            name="executor_message",
            log_file="logs/executor/executor_binance.log"
        )

        self.symbols:list[str] = ["BTC/USDT"]

        self.group_name:str = "executor_data"
        self.consumer_name = "executor_01"
        self.data_types:list[str] = ["orderbook","trades","market_price","open_interect"]
        self.streamings:dict[str,str] = {}

        self.stream_keys = StreamKeys()
        self.strategy = TakerTrendStrategy()
        self.data_manager = DataManager(self.symbols)
        self.state = State(self.symbols)
        self.risk = Risk()
        self.exchange = Exchange(
            logger=self.logger,
            symbols=self.symbols
        )

    async def init_account_data(self) -> bool:
        try:
            account = await self.exchange.get_account_data()
            self.state.update_account_data(account)
            return True if self.state.get_account()["asset"] is not None else False
        except Exception as e:
            self.logger.error(f"init data error: {e}")
            return False

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
        init_ok = await self.init_account_data()
        if init_ok:
            while True:
                if not self.streamings:
                    await asyncio.sleep(1)
                    continue

                response = await self.redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams=self.streamings,
                    count=500,
                    block=2000
                )

                if response:
                    for stream_name,messages in response:
                        for msg_id,content in messages:
                            data = json.loads(content["data"])
                            result = self.data_manager.main(stream_key=stream_name,data=data)
                            self.redis.xack(stream_name,self.group_name,msg_id)
                            if result is None: continue

                            symbol,features,snapshot = result
                            self.state.update_market_data(symbol,features,snapshot)

                            signal = self.strategy.evaluate(symbol,self.state)
                            
                            if signal:
                                self.risk

    async def run(self):
        await self.redis.sadd("registry:streams:orderbook","md:binance:spot:BTC-USDT:orderbook")
        
        tasks = []

        tasks.append(asyncio.create_task(self.refresh_stream_keys()))
        tasks.append(asyncio.create_task(self.consume_data()))

        await asyncio.gather(*tasks)

if __name__ == "__main__":
    obj = Executor()
    asyncio.run(obj.run())