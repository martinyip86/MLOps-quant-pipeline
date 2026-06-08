from src.storage.redis.client import redis_manager
from src.utils.logger import setup_logger
from src.executor.feature_state import FeatureState
from src.executor.data_manager import DataManager
from src.strategies.taker_trend_strategy import TakerTrendStrategy

import asyncio
import json

class RealTimeExecutor:
    def __init__(self):
        self.redis = redis_manager.connect
        self.logger = setup_logger(
            name="executor_message",
            log_file="logs/executor/executor_binance.log"
        )

        self.group_name = "executor_data"
        self.consumername = "executor_01"
        self.streamings:dict[str,str] = {}
        self.symbols = ["BTC/USDT"]

        self.state = FeatureState(self.symbols)
        self.data_manager = DataManager(self.symbols)
        self.strategy = TakerTrendStrategy()

    async def refresh_stream_keys(self):
        while True:
            for data_type in ['orderbook','trades','market_price','open_interest']:
                registry = f"registry:streams:{data_type}"
                remote_keys = await self.redis.smembers(registry)
                if remote_keys:
                    for remote_key in remote_keys:
                        try:
                            if remote_key not in self.streamings:
                                await self.redis.xgroup_create(
                                    name=remote_key,
                                    groupname=self.group_name,
                                    id="0",
                                    mkstream=True
                                )
                                self.streamings[remote_key] = ">"
                                self.logger.info(f"✅ Created group {self.group_name} for {remote_key}")
                        except Exception as e:
                            if "BUSYGROUP" in str(e):
                                self.streamings[remote_key] = ">"

            await asyncio.sleep(30)

    async def consume_market_data(self):
        while True:
            if not self.streamings:
                await asyncio.sleep(1)
                continue

            response = await self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumername,
                streams=self.streamings,
                count=500,
                block=2000
            )

            if response:
                for stream_name,messages in response:
                    for msg_id,content in messages:
                        data = json.loads(content['data'])
                        result = self.data_manager.main(stream_name,data)
                        await self.redis.xack(stream_name,self.group_name,msg_id)
                        if result is None: continue
                            
                        symbol,features,snapshot = result
                        self.state.update_market(symbol,features,snapshot)

                        signal = self.strategy.evaluate(symbol,self.state)

                        if signal:
                            self.logger.info(f"[{signal.symbol}][{signal.side}-{signal.action}] confidence: {signal.confidence} | notional_usd: {signal.notional_usd} | expected_edge_bps: {signal.expected_edge_bps} | cost_bps: {signal.cost_bps}")
                            self.logger.info(f"----{signal.reason}")

    async def main(self):
        tasks = []
        tasks.append(asyncio.create_task(self.refresh_stream_keys()))
        tasks.append(asyncio.create_task(self.consume_market_data()))
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    obj = RealTimeExecutor()
    asyncio.run(obj.main())