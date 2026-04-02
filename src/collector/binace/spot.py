from src.collector.base.stream_base import StreamBase
from src.storage.redis.client import redis_manager
from src.models.schema import TickData,TradeData
from src.utils.logger import setup_logger
from src.monitoring.metrics import ws_reconnect_total,ws_error_total,silence_gauge
import ccxt.pro as ccxt_pro
import asyncio
import time
import sys

class BinanceSpotWsManager(StreamBase):
    def __init__(self, exchange_id, mkt_type):
        super().__init__(exchange_id, mkt_type)
        self.logger = setup_logger(
            name=f'ws_collector_{exchange_id}_{mkt_type}',
            log_file=f"logs/collector/collector_{exchange_id}_{mkt_type}.log"
        )
        self.redis = redis_manager.connect
        self._is_reconnecting = False
        self._reconnect_lock = asyncio.Lock()

    async def connect(self):
        async with self._reconnect_lock:
            if not self._is_reconnecting and self.ws:
                    return
            try:
                if self.ws:
                    await self.ws.close()
                self.logger.info(f"🔄 [RECONNECT] Initializing new CCXT Pro client for {self.exchange_id}...")
                self.ws = ccxt_pro.binance({
                    'enableRateLimit':True,
                    'options':{
                        'defaultType':'spot',
                        'ws': { 
                            "heartbeat": 20000 
                        }
                    }
                })
                await asyncio.sleep(0.01)
                self.logger.info("✅ [SUCCESS] Connection established.")
            except Exception as e:
                self.logger.error(f"❌ [RECONNECT-FAILED] {e}")
                raise e
            finally:
                self._is_reconnecting = False

    async def watch_loop(self,symbol,method_name):
        retry_delay = 1
        last_active = time.time()
        while True:
            try:
                if self._is_reconnecting:
                    await asyncio.sleep(1)
                    continue

                method = getattr(self.ws,method_name)
                data = await asyncio.wait_for(method(symbol),timeout=25)

                last_active = time.time()
                retry_delay = 1 # 成功后重置退避时间

                await self.queue.put({
                    'type':'orderbook' if 'book' in method_name else 'trades',
                    'symbol':symbol,
                    'data':data
                })
            except (asyncio.TimeoutError, Exception) as e:
                silence_gap = time.time() - last_active
                silence_gauge.labels(
                    exchange_id=self.exchange_id,
                    mkt_type=self.mkt_type,
                    symbol=symbol,
                    method_name=method_name
                ).set(silence_gap)

                self.logger.error(f"⚠️ {symbol} {method_name} Error: {e} (Silence: {silence_gap:.1s}s)")
                ws_error_total.labels(exchange=self.exchange_id,mkt_type=self.mkt_type,symbol=symbol).inc()
                
                is_network_error = any(msg in str(e).lower() for msg in ['closed', 'reset', 'disconnected', 'none type'])

                if silence_gap > 60 or is_network_error:
                    if not self._is_reconnecting:
                        self._is_reconnecting = True
                        self.logger.warning(f"🚨 [FATAL] {symbol} {method_name} dead. Triggering global reconnect...")
                        ws_reconnect_total.labels(
                            exchange_id=self.exchange_id,
                            mkt_type=self.mkt_type,
                            symbol=symbol,
                            method_name=method_name
                        ).inc()
                        await self.connect()

                    last_active = time.time()
                    continue

                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60) # 指数退避


    async def _handle_orderbook(self,symbol:str,data):
        stream_key = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:orderbook"
        registry = f"registry:streams:orderbook"
        ts = data.get('timestamp',time.time() * 1000)
        tick = TickData(
            exchange_id=self.exchange_id,
            symbol=symbol,
            mkt_type=self.mkt_type,
            bid_price=data['bids'][0][0],
            bid_volume=data['bids'][0][1],
            ask_price=data['asks'][0][0],
            ask_volume=data['asks'][0][1],
            bid_prices=[row[0] for row in data['bids'][:20]],
            bid_volumes=[row[1] for row in data['bids'][:20]],
            ask_prices=[row[0] for row in data['asks'][:20]],
            ask_volumes=[row[1] for row in data['asks'][:20]],
            timestamp=ts
        )
        await self.redis.sadd(registry,stream_key)
        await self.redis.xadd(stream_key,{'data':tick.model_dump_json()},maxlen=10000,approximate=True)

    async def _handle_trades(self,symbol:str,trades):
        stream_key = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:trades"
        registry_key = f"registry:streams:trades"
        for trade_dict in trades:
            info = trade_dict.get('info', {})
            is_m = info.get('m', str(info.get('isBuyerMaker', '')).lower() == 'true')
            is_m_bool = str(is_m).lower() == 'true'
            is_taker_buyer = not is_m_bool
            trade = TradeData(
                exchange_id=self.exchange_id,
                symbol=symbol,
                mkt_type=self.mkt_type,
                trade_id=int(trade_dict['id']),
                trade_id_raw=str(trade_dict['id']),
                timestamp=trade_dict.get('timestamp',int(time.time() * 1000)),
                side=trade_dict['side'],
                price=trade_dict['price'],
                amount=trade_dict['amount'],
                is_taker_buyer=is_taker_buyer
            )
            await self.redis.sadd(registry_key,stream_key)
            await self.redis.xadd(stream_key,{'data':trade.model_dump_json()},maxlen=10000,approximate=True)

    async def route(self):
        while True:
            try:
                msg = await self.queue.get()
                
                data_type = msg['type']
                if data_type == 'orderbook':
                    await self._handle_orderbook(msg['symbol'],msg['data'])
                elif data_type == 'trades':
                    await self._handle_trades(msg['symbol'],msg['data'])
                    
                self.queue.task_done()
            except Exception as e:
                self.logger.error(f"route have error: {e}")
                ws_error_total.labels(exchange=self.exchange_id,mkt_type=self.mkt_type,symbol=msg['symbol']).inc()
                await asyncio.sleep(0.1)
