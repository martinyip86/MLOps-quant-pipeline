import polars as pl
import numpy as np
import asyncio
import json
from collections import deque

from src.storage.redis.client import redis_manager
from src.utils.logger import setup_logger
from src.strategies.high_freq_taker_strategy import HighFreqTakerStrategy as hfts
from src.analytics.generate_features import generate_maker_features

class HighFreqTakerStrategyExecutor:
    def __init__(self,symbols:list[str]):
        self.redis = redis_manager.connect
        self.symbols = symbols
        self.exchange_id = 'binance'

        self.logger = setup_logger(
            name='high_freq_taker_strategy',
            log_file='logs/executor/high_freq_taker_strategy.log'
        )

        # 🌟 核心防线：Executor 专属消费者组，确保与 Syncer 互不干扰，独自享用全量广播流
        self.group_name = 'executor_strategy_group'
        self.consumer_name = 'executor_instance_01'

        # 动态跟踪我们关心的 Streams
        self.streaming_keys = {}

        # 1. 为每种数据流定义内存队列（保持最近 5-10 秒的数据就足够满足你 rolling_sum(50) 的需求了）
        # 假设 100ms 一个点，50个点是 5秒，存 200 个点（20秒）足够安全
        self.buffers = {
            symbol:{
                'orderbook_spot': deque(maxlen=200),
                'orderbook_future': deque(maxlen=200),
                'trades_spot': deque(maxlen=1000),      # 成交可能密集，容量给大点
                'trades_future': deque(maxlen=1000),
                'mark_price_future': deque(maxlen=200),
                'open_interest_future': deque(maxlen=200)
            } for symbol in symbols
        }

        self.strategies = {symbol: hfts() for symbol in symbols}

    async def _get_redis_streaming_key(self):
        while True:
            for w_type in ['orderbook','trades','market_price','open_interest']:
                registry = f"registry:streams:{w_type}"
                remote_keys = await self.redis.smembers(registry)
                for remote_key in remote_keys:
                    if remote_key not in self.streaming_keys:
                        try:
                            await self.redis.xgroup_create(
                                name=remote_key,
                                groupname=self.group_name,
                                id='0',
                                mkstream=True
                            )
                            self.logger.info(f"✅ Created group {self.group_name} for {remote_key}")
                            self.streaming_keys[remote_key] = ">"
                        except Exception as e:
                            if "BUSYGROUP" in str(e):
                                self.streaming_keys[remote_key] = ">"
                            else:
                                self.logger.error(f"❌ [REGISTRY-ERROR] {e}")
                                await asyncio.sleep(5)

                await asyncio.sleep(30)

    async def _distribute_data(self):
        pending_ack = {}
        while True:
            if not self.streaming_keys:
                await asyncio.sleep(1)
                continue

            response = await self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumer_name,
                streams=self.streaming_keys,
                count=500,
                block=2000
            )

            if response:
                for stream_name,message in response:
                    if stream_name not in pending_ack:
                        pending_ack[stream_name] = []

                    parts = stream_name.split(':')
                    mkt_type = parts[-3]
                    symbol = parts[-2].replace('-','/')
                    data_type = parts[-1]

                    for msg_id,content in message:
                        pending_ack[stream_name].append(msg_id)
                        self.buffers[symbol][f"{data_type}_{mkt_type}"].append(json.loads(content['data']))
                        await self.redis.xack(stream_name, self.group_name, msg_id)

    async def executor(self):
        MIN_REQUIRED_SAMPLES = 100
        while True:
            await asyncio.sleep(0.1)
            # 确保 6 个队列都有基本数据才开始连表
            for symbol in self.symbols:
                buffers = self.buffers[symbol]

                is_ready = all(
                    len(queue) >= MIN_REQUIRED_SAMPLES
                    for queue in buffers.values()
                )

                if not is_ready:
                    # 如果数据不够，打印 debug 日志展示进度，然后跳过该币种
                    current_status = {k:len(v) for k,v in buffers.items()}
                    self.logger.debug(f"⏳ [{symbol}] 数据预热中... 当前进度: {current_status}")
                    continue

                # 🔥 数据已就绪，开始转换为 Polars LazyFrame 并计算特征
                try:
                    # 将内存中的 dict 列表快照转换
                    df_orderbook_spot = pl.DataFrame(list(buffers['orderbook_spot'])).lazy()
                    df_orderbook_future = pl.DataFrame(list(buffers['orderbook_future'])).lazy()
                    df_trades_spot = pl.DataFrame(list(buffers['trades_spot'])).lazy()
                    df_trades_future = pl.DataFrame(list(buffers['trades_future'])).lazy()
                    df_mark_price_future = pl.DataFrame(list(buffers['mark_price_future'])).lazy()
                    df_open_interest_future = pl.DataFrame(list(buffers['open_interest_future'])).lazy()

                    # 调用你的特征生成函数
                    df = generate_maker_features(
                        df_orderbook_spot=df_orderbook_spot,
                        df_orderbook_future=df_orderbook_future,
                        df_trades_spot=df_trades_spot,
                        df_trades_future=df_trades_future,
                        df_mark_price=df_mark_price_future,
                        df_open_interest=df_open_interest_future
                    )
                    row = df.collect().tail(1).row(0,named=True)

                    signals = []
                    for strategy in self.strategies[symbol]:
                        signal = strategy.on_features()


                except Exception as e:
                    self.logger.error(f"❌ [{symbol}] Polars pipeline 运行报错: {e}", exc_info=True)

    async def main(self):
        tasks = [
            asyncio.create_task(self._get_redis_streaming_key()),
            asyncio.create_task(self._distribute_data()),
            asyncio.create_task(self.executor())
        ]

        await asyncio.gather(*tasks)