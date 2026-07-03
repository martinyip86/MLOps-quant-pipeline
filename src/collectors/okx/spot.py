from src.collectors.base.stream_base import StreamBase
from src.models.schema import TickData,TradeData
import ccxt.pro as ccxt_pro
import asyncio
import time

class OkxSpotWsManager(StreamBase):
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
                    # Clear the old client before reconnecting so a failed new
                    # client does not leave a closed ws object behind.
                    self.ws = None
                self.logger.info(f"🔄 [RECONNECT] Initializing new CCXT Pro client for {self.exchange_id}...")
                self.ws = ccxt_pro.okx({
                    'enableRateLimit':True,
                    'options':{
                        'defaultType':'spot',
                        'ws': { 
                            "heartbeat": 5000 
                        }
                    }
                })
                await asyncio.sleep(0.01)
                self.logger.info("✅ [SUCCESS] Connection established.")
            except Exception as e:
                self.ws = None
                self.logger.error(f"❌ [RECONNECT-FAILED] {e}")
                raise e
            finally:
                self._is_reconnecting = False

    async def _handle_orderbook(self,symbol:str,data):
        stream_key = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:orderbook"
        registry = f"registry:streams:orderbook"
        raw_ts = data.get('timestamp')
        ts = raw_ts if raw_ts is not None else int(time.time() * 1000)
        await self.redis.sadd(registry,stream_key)
        try:
            async with self.redis.pipeline(transaction=False) as pipe:
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
                await pipe.xadd(stream_key,{'data':tick.model_dump_json()},maxlen=5000,approximate=True)
                await pipe.execute()
        except Exception as e:
            self.logger.error(f"orderbook add redis error: {e}")

    async def _handle_trades(self,symbol:str,trades):
        stream_key = f"md:{self.exchange_id}:{self.mkt_type}:{symbol.replace('/','-')}:trades"
        registry_key = f"registry:streams:trades"
        await self.redis.sadd(registry_key,stream_key)
        try:
            async with self.redis.pipeline(transaction=False) as pipe:
                for trade_dict in trades:
                    is_taker_buyer = trade_dict.get('side') == 'buy'
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
                    await pipe.xadd(stream_key,{'data':trade.model_dump_json()},maxlen=5000,approximate=True)
                await pipe.execute()
        except Exception as e:
            self.logger.error(f"trades add redis error: {e}")
