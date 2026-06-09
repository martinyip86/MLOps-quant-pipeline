from src.collectors.base.stream_base import StreamBase
from src.models.schema import TickData,TradeData
import ccxt.pro as ccxt_pro
import asyncio
import time

class BinanceSpotWsManager(StreamBase):
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
                await asyncio.sleep(5)
            finally:
                self._is_reconnecting = False

    async def _handle_orderbook(self,symbol:str,data):
        stream_key = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:orderbook"
        registry_key = f"registry:streams:orderbook"
        await self.register_stream_once(registry_key,stream_key)
        raw_ts = data.get('timestamp')
        ts = raw_ts if raw_ts is not None else int(time.time() * 1000)
        try:
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
                    info = trade_dict.get('info', {})
                    is_m = info.get('m', str(info.get('isBuyerMaker', '')).lower() == 'true')
                    is_m_bool = str(is_m).lower() == 'true'
                    is_taker_buyer = not is_m_bool
                    raw_ts = trade_dict.get('timestamp')
                    ts = raw_ts if raw_ts is not None else int(time.time() * 1000)
                    trade = TradeData(
                        exchange_id=self.exchange_id,
                        symbol=symbol,
                        mkt_type=self.mkt_type,
                        trade_id=int(trade_dict['id']),
                        trade_id_raw=str(trade_dict['id']),
                        timestamp=ts,
                        side=trade_dict['side'],
                        price=trade_dict['price'],
                        amount=trade_dict['amount'],
                        is_taker_buyer=is_taker_buyer
                    )
                    
                    await pipe.xadd(stream_key,{'data':trade.model_dump_json()},maxlen=10000,approximate=True)
                await pipe.execute()
        except Exception as e:
            self.logger.error(f"trades add redis error: {e}")

