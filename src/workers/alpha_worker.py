import asyncio
import orjson
from dataclasses import asdict
import sys

from src.analytics.alpha_model import AlphaModel,AlphaSignal
from src.storage.redis.client import redis_manager
from src.utils.logger import setup_logger

class AlphaSignalWorker:
    def __init__(self):
        self.redis = redis_manager.connect
        self.logger = setup_logger("alpha_worker","logs/workers/alpha_worker.log")
        self.group_name = "alpha_signal_group"
        self.consumer_name = "alpha_worker_01"
        self.exchange_id = "binance"
        self.mkt_type = "spot"
        self.watch_type = "orderbook"
        self.symbols = ["BTC/USDT"]
        self.models = {}

    async def init_models(self):
        for symbol in self.symbols:
            try:
                self.models[symbol] = AlphaModel(self.exchange_id,self.mkt_type,symbol,self.watch_type)
            except Exception as e:
                self.logger.error(f"❌ 模型初始化失败 {symbol}: {e}")

    async def main(self):
        await self.init_models()

        streams = {}
        for symbol in self.symbols:
            stream_key = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:{self.watch_type}"
            try:
                await self.redis.xgroup_create(stream_key, self.group_name, id='0', mkstream=True)
            except: pass
            streams[stream_key] = ">"

        self.logger.info(f"🚀 Alpha Worker 运行中... 监听: {list(streams.keys())}")

        while True:
            try:
                response = await self.redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=self.consumer_name,
                    streams=streams,
                    count=20,
                    block=1000
                )

                if not response: continue

                for stream_name,messages in response:
                    s_key = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
                    symbol = s_key.split(":")[-2].replace('-','/')
                    msg_ids = []
                    for msg_id,content in messages:
                        try:
                            tick = orjson.loads(content['data'])
                            signal = self.models[symbol].generate_signal(tick)

                            if signal:
                                signal_key = f"alpha:signals:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/', '-')}"
                                await self.redis.xadd(
                                    signal_key,
                                    {"data":orjson.dumps(asdict(signal))},
                                    maxlen=10000,
                                    approximate=True
                                )
                                if signal.recommendation != "NEUTRAL":
                                    self.logger.info(f"🔥 [{signal.recommendation}] {symbol} | Conf: {signal.confidence:.2f} | Spread: {signal.metadata['spread_bps']:.1f}")
                            msg_ids.append(msg_id)
                        except Exception as e:
                            self.logger.error(f"处理消息失败: {e}")

                    if msg_ids:
                        await self.redis.xack(stream_name,self.group_name,*msg_ids)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Worker 主循环异常: {e}")
                await asyncio.sleep(1)


if __name__=="__main__":
    worker = AlphaSignalWorker()
    try:
        asyncio.run(worker.main())
    except KeyboardInterrupt:
        print("\n🛑 Alpha Worker 已停止")
    except Exception as e:
        print(f"启动失败: {e}")