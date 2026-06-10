from src.collectors.base.stream_base import StreamBase
from src.models.schema import TradeDataForFuture,MarketPriceData,OpenInterestData,FundingRateData,OrderbookForFuture,LiquidationsData
import ccxt.pro as ccxt_pro
import asyncio
import time

class BinanceFutureManager(StreamBase):
    def __init__(self, exchange_id, mkt_type):
        super().__init__(exchange_id, mkt_type)

    async def connect(self):
        async with self._reconnect_lock:
            if not self._is_reconnecting and self.ws:
                return
            try:
                if self.ws:
                    self.logger.info(f"🔄 [CLOSE] Close old CCXT Pro client for {self.exchange_id}")
                    await self.ws.close()
                self.logger.info(f"🔄 [RECONNECT] Initializing new CCXT Pro client for {self.exchange_id}...")
                self.ws = ccxt_pro.binanceusdm({
                    'enableRateLimit':True,
                    'options':{
                        'defaultType':'swap',
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

    async def _handle_orderbook(self,symbol:str,data):
        stream_key = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:orderbook"
        registry_key = f"registry:streams:orderbook"
        await self.register_stream_once(registry_key,stream_key)
        raw_ts = data.get('timestamp')
        ts = raw_ts if raw_ts is not None else int(time.time() * 1000)
        try:
            tick = OrderbookForFuture(
                exchange_id=self.exchange_id,
                symbol=symbol,
                mkt_type=self.mkt_type,
                bid_prices=[row[0] for row in data['bids'][:20]],
                bid_volumes=[row[1] for row in data['bids'][:20]],
                ask_prices=[row[0] for row in data['asks'][:20]],
                ask_volumes=[row[1] for row in data['asks'][:20]],
                nonce=data['nonce'],
                timestamp=ts
            )
            await self.redis.xadd(stream_key,{'data':tick.model_dump_json()},maxlen=1000,approximate=True)
        except Exception as e:
            self.logger.error(f"orderbook add redis error: {e}")

    async def _handle_trades(self,symbol:str,trades):
        stream_key = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:trades"
        registry_key = f"registry:streams:trades"
        await self.register_stream_once(registry_key,stream_key)
        try:
            async with self.redis.pipeline(transaction=False) as pipe:
                for trade_dict in trades:
                    if trade_dict['price'] > 0 and trade_dict['amount']:
                        raw_ts = trade_dict.get('timestamp')
                        ts = raw_ts if raw_ts is not None else int(time.time() * 1000)
                        trade = TradeDataForFuture(
                            exchange_id=self.exchange_id,
                            symbol=symbol,
                            mkt_type=self.mkt_type,
                            trade_id=int(trade_dict['id']),
                            timestamp=ts,
                            side=trade_dict['side'],
                            price=trade_dict['price'],
                            amount=trade_dict['amount']
                        )
                        await pipe.xadd(stream_key,{'data':trade.model_dump_json()},maxlen=10000,approximate=True)
                await pipe.execute()
        except Exception as e:
            self.logger.error(f"future trades add redis error: {e}")

    async def _handle_market_price(self,symbol:str,data):
        stream_key_mp = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:market_price"
        registry_mp = f"registry:streams:market_price"
        await self.register_stream_once(registry_mp,stream_key_mp)
        stream_key_fr = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:funding_rate"
        registry_fr = f"registry:streams:funding_rate"
        await self.register_stream_once(registry_fr,stream_key_fr)
        try:
            async with self.redis.pipeline(transaction=False) as pipe:
                info = data.get('info')
                raw_ts = data.get('timestamp')
                ts = raw_ts if raw_ts is not None else int(time.time() * 1000)
                marketPriceData = MarketPriceData(
                    exchange_id=self.exchange_id,
                    symbol=symbol,
                    mkt_type=self.mkt_type,
                    mark_price=data['markPrice'],
                    timestamp=ts,
                    index_price=data['indexPrice']
                )
                fundingRateData = FundingRateData(
                    exchange_id=self.exchange_id,
                    symbol=symbol,
                    mkt_type=self.mkt_type,
                    timestamp=ts,
                    funding_rate=info['r'],
                    next_funding_rate_timestamp=info['T']
                )
                await pipe.xadd(stream_key_mp,{'data':marketPriceData.model_dump_json()},maxlen=500,approximate=True)
                await pipe.xadd(stream_key_fr,{'data':fundingRateData.model_dump_json()},maxlen=100,approximate=True)
                await pipe.execute()
        except Exception as e:
            self.logger.error(f"mp add redis error: {e}")

    async def fetch_open_interest(self,symbol:str,sleep_time:int=30):
        split_symbol = symbol.split('/')
        future_symbol = f"{split_symbol[0]}/{split_symbol[1]}:{split_symbol[1]}"
        stream_key = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:open_interest"
        registry = f"registry:streams:open_interest"
        await self.register_stream_once(registry,stream_key)
        while True:
            try:
                data = await asyncio.wait_for(self.ws.fetch_open_interest(future_symbol),timeout=60)
                raw_ts = data.get('timestamp')
                ts = raw_ts if raw_ts is not None else int(time.time() * 1000)
                oiData = OpenInterestData(
                    exchange_id=self.exchange_id,
                    symbol=symbol,
                    mkt_type=self.mkt_type,
                    timestamp=ts,
                    base_volume=data['baseVolume'],
                    open_interest_amount=data['openInterestAmount']
                )
                await self.redis.xadd(stream_key,{'data':oiData.model_dump_json()},maxlen=300,approximate=True)
                await asyncio.sleep(sleep_time)
            except Exception as e:
                self.logger.error(f"oi add redis error: {e}")
                await asyncio.sleep(10)

    async def watch_liquidations(self,symbol:str):
        split_symbol = symbol.split('/')
        future_symbol = f"{split_symbol[0]}/{split_symbol[1]}:{split_symbol[1]}"
        stream_key = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:liquidations"
        registry = f"registry:streams:liquidations"
        await self.register_stream_once(registry,stream_key)
        while True:
            try:
                data = await self.ws.watch_liquidations(future_symbol)
                
                async with self.redis.pipeline(transaction=False) as pipe:
                    for item in data:
                        info = item['info']
                        lq_data = LiquidationsData(
                            exchange_id=self.exchange_id,
                            symbol=symbol,
                            price=info['p'],
                            amount=info['q'],
                            side=info['S'],
                            time_in_force=info['f'],
                            order_status=info['X'],
                            timestamp=info['T']
                        )
                        await pipe.xadd(stream_key,{'data':lq_data.model_dump_json()},maxlen=3000,approximate=True)
                    
                    await pipe.execute()

            except Exception as e:
                self.logger.error(f"liquidations add redis error: {e}")
                if data is not None:
                    print(data)
                await asyncio.sleep(10)