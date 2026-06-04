from src.storage.redis.client import redis_manager
from src.utils.logger import setup_logger
from src.executor.feature_state import FeatureState

import asyncio

class RealTimeExecutor:
    def __init__(self):
        self.redis = redis_manager.connect
        self.logger = setup_logger(
            name="executor_message",
            log_file="executor/executor_binance.log"
        )

        self.group_name = "executor_data"
        self.consumername = "executor_01"
        self.streamings:dict[str,str] = {}
        self.symbols = ["BTC/USDT"]

        self.state = FeatureState(self.symbols)

    async def refresh_stream_keys(self):
        while True:
            for data_type in ['orderbook','trades','market_price','open_interest']:
                registry = f"registry:streams:{data_type}"
                remote_keys = await self.redis.smembers(registry)
                if remote_keys:
                    for remote_key in remote_keys:
                        try:
                            if remote_key not in self.streamings:
                                self.redis.xgroup_create(
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

            response = self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumername,
                streams=self.streamings,
                count=500,
                block=2000
            )

            if response:
                for stream_name,messages in response:
                    for msg_id,content in messages:
                        self.state()