from abc import ABC,abstractmethod
from src.utils.logger import setup_logger
from src.storage.redis.client import redis_manager
from src.monitoring.metrics import ws_reconnect_total,silence_gauge,ws_error_total
from aiohttp import web
import asyncio
import time


class StreamBase(ABC):
    def __init__(self,exchange_id:str,mkt_type:str):
        self.exchange_id:str = exchange_id
        self.mkt_type:str = mkt_type
        self.queue = asyncio.Queue(maxsize=5000)
        self.logger = setup_logger(
            name=f'ws_collector_{exchange_id}_{mkt_type}',
            log_file=f"logs/collector/collector_{exchange_id}_{mkt_type}.log"
        )
        self.last_time = time.time()
        self.redis = redis_manager.connect
        self._is_reconnecting = False
        self._reconnect_lock = asyncio.Lock()
        self.ws = None

    @abstractmethod
    async def connect(self):
        pass

    async def watch_loop(self,symbol,method_name,watch_name):
        retry_delay = 1
        last_active = time.time()
        is_active = True
        while True:
            try:
                if self._is_reconnecting or not self.ws:
                    await asyncio.sleep(1)
                    continue

                method = getattr(self.ws,method_name)
                data = await asyncio.wait_for(method(f"{symbol}:USDT" if self.mkt_type == 'future' else symbol),timeout=60)

                self.last_time = time.time()
                last_active = time.time()
                retry_delay = 1 # 成功后重置退避时间

                if not is_active:
                    is_active = True
                    silence_gauge.labels(
                        exchange=self.exchange_id,
                        mkt_type=self.mkt_type,
                        symbol=symbol,
                        method_name=method_name
                    ).set(0)
                    self.logger.info(f"{symbol} {method_name} reconnect success")

                await self.queue.put({
                    'type':watch_name,
                    'symbol':symbol,
                    'data':data
                })
            except (asyncio.TimeoutError, Exception) as e:
                is_active = False
                silence_gap = time.time() - last_active

                if self._is_reconnecting and silence_gap < 5:
                    self.logger.debug(f"ℹ️ {symbol} {method_name} suppressed during global reconnect.")
                else:
                    silence_gauge.labels(
                        exchange=self.exchange_id,
                        mkt_type=self.mkt_type,
                        symbol=symbol,
                        method_name=method_name
                    ).set(silence_gap)

                    self.logger.error(f"⚠️ {symbol} {method_name} Error: {e} (Silence: {silence_gap:.1f}s)")
                    ws_error_total.labels(exchange=self.exchange_id,mkt_type=self.mkt_type,symbol=symbol).inc()
                    
                    is_timeout = isinstance(e, asyncio.TimeoutError)
                    is_network_error = any(msg in str(e).lower() for msg in ['closed', 'reset', 'disconnected', 'none type'])

                    if silence_gap > 61 or is_network_error or is_timeout:
                        if not self._is_reconnecting:
                            self._is_reconnecting = True
                            self.logger.warning(f"🚨 [FATAL] {symbol} {method_name} dead. Triggering global reconnect...")
                            ws_reconnect_total.labels(
                                exchange=self.exchange_id,
                                mkt_type=self.mkt_type,
                                symbol=symbol,
                                method_name=method_name
                            ).inc()
                            await self.connect()

                        last_active = time.time()

                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 60) # 指数退避

    async def route(self):
        while True:
            try:
                msg = await self.queue.get()
                
                data_type = msg['type']
                if data_type == 'orderbook' and self.mkt_type == 'spot':
                    await self._handle_orderbook(msg['symbol'],msg['data'])
                elif data_type == 'trades':
                    await self._handle_trades(msg['symbol'],msg['data'])
                elif data_type == 'mark_price':
                    await self._handle_market_price(msg['symbol'],msg['data'])
                    
                self.queue.task_done()
            except Exception as e:
                self.logger.error(f"route have error: {e}")
                ws_error_total.labels(exchange=self.exchange_id,mkt_type=self.mkt_type,symbol=msg['symbol']).inc()
                await asyncio.sleep(0.1)

    async def start_health_check(self,port=8080):

        async def handle(_request):
            silence_duration = time.time() - self.last_time
            if silence_duration > 120:
                return web.Response(status=500,text=f"Data Silence: {silence_duration:.2f}s")
            return web.Response(status=200,text="OK")
        
        app = web.Application()
        app.router.add_get('/health',handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner,'0.0.0.0',port)
        print(f"✅ Health check server started at : {port}/health")
        await site.start()