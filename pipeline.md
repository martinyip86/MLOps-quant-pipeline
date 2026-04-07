manager.py:
from src.collectors.binace.spot import BinanceSpotWsManager
from src.collectors.okx.spot import OkxSpotWsManager
from src.monitoring.pusher import start_metrics_pusher
import os
import asyncio
import argparse

class Manager:
    def __init__(self,exchange_id:str,mkt_type:str):
        self.exchange_id:str = exchange_id
        self.mkt_type:str = mkt_type
        self.symbols = ['BTC/USDT','ETH/USDT']
        self._collector_map = {
            ('binance','spot'):BinanceSpotWsManager,
            ('okx','spot'):OkxSpotWsManager,
        }

    async def main(self):
        tasks = []
        collector_class = self._collector_map.get((self.exchange_id,self.mkt_type))
        if not collector_class:
            print(f"Error: {self.exchange_id} {self.mkt_type} 不在支持列表中")
            return
        controller = collector_class(self.exchange_id,self.mkt_type)
        await controller.connect()
                
        for symbol in self.symbols:
            tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_order_book')))
            tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_trades')))

        tasks.append(asyncio.create_task(controller.route()))
        tasks.append(asyncio.create_task(start_metrics_pusher(job_name=f"market_collector_{self.exchange_id}_{self.mkt_type}")))
                   
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--exchange',type=str,default=os.getenv('EXCHANGE', 'binance'))
    parser.add_argument('--type',type=str,default=os.getenv('TYPE', 'spot'))
    args = parser.parse_args()
    manager = Manager(exchange_id=args.exchange,mkt_type=args.type)
    
    try:
        asyncio.run(manager.main())
    except KeyboardInterrupt:
        print("停止采集...")

stream_base.py;
from abc import ABC,abstractmethod
from src.utils.logger import setup_logger
from src.storage.redis.client import redis_manager
from src.monitoring.metrics import ws_reconnect_total,silence_gauge,ws_error_total
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
        self.redis = redis_manager.connect
        self._is_reconnecting = False
        self._reconnect_lock = asyncio.Lock()
        self.ws = None

    @abstractmethod
    async def connect(self):
        pass

    @abstractmethod
    async def _handle_orderbook(self):
        pass

    @abstractmethod
    async def _handle_trades(self):
        pass

    async def watch_loop(self,symbol,method_name):
        retry_delay = 1
        last_active = time.time()
        is_active = True
        while True:
            try:
                if self._is_reconnecting or not self.ws:
                    await asyncio.sleep(1)
                    continue

                method = getattr(self.ws,method_name)
                data = await asyncio.wait_for(method(symbol),timeout=60)

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
                    'type':'orderbook' if 'book' in method_name else 'trades',
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
                if data_type == 'orderbook':
                    await self._handle_orderbook(msg['symbol'],msg['data'])
                elif data_type == 'trades':
                    await self._handle_trades(msg['symbol'],msg['data'])
                    
                self.queue.task_done()
            except Exception as e:
                self.logger.error(f"route have error: {e}")
                ws_error_total.labels(exchange=self.exchange_id,mkt_type=self.mkt_type,symbol=msg['symbol']).inc()
                await asyncio.sleep(0.1)

binance-spot.py:
from src.collectors.base.stream_base import StreamBase
from src.models.schema import TickData,TradeData
import ccxt.pro as ccxt_pro
import asyncio
import time
import orjson

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
                await pipe.xadd(stream_key,{'data':tick.model_dump_json()},maxlen=10000,approximate=True)
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

syncer.py:
from src.storage.redis.client import redis_manager
from src.storage.clickhouse.client import ch_manager
from src.utils.logger import setup_logger
from src.monitoring.pusher import start_metrics_pusher
from src.monitoring.metrics import parquet_write_duration,redis_mem_gauge,parquet_write_bytes
import polars as pl
import asyncio
import json
import time

class Syncer:
    def __init__(self):
        self.redis = redis_manager.connect
        self.ch = ch_manager.connect
        self.streaming_keys = {}
        self.logger = setup_logger(
            name="worker_syncer",
            log_file="logs/workers/worker_syncer.log"
        )
        self.group_name = "ch_syncer_group"
        self.batch_size = 10000
        self.flush_interval = 10.0

    async def _get_redis_streaming_key(self):
        while True:
            try:
                for w_type in ['orderbook','trades']:
                    registry = f"registry:streams:{w_type}"
                    remote_keys = await self.redis.smembers(registry)
                    for remote_key in remote_keys:
                        rekey = remote_key.decode() if isinstance(remote_key,bytes) else remote_key
                        if rekey not in self.streaming_keys:
                            await self.redis.xgroup_create(
                                name=rekey,
                                groupname=self.group_name,
                                id='0',
                                mkstream=True
                            )
                            self.logger.info(f"✅ Created group {self.group_name} for {remote_key}")
                            self.streaming_keys[rekey] = ">"

                await asyncio.sleep(30)
            except Exception as e:
                self.logger.error(f"❌ [REGISTRY-ERROR] {e}")
                await asyncio.sleep(5)


    async def storage_worker(self):
        buffer = []
        pending_ack = {}
        last_flush = time.time()
        while True:
            if not self.streaming_keys:
                await asyncio.sleep(1) # 快速轮询等待初始化
                continue

            response = await self.redis.xreadgroup(
                groupname=self.group_name,
                consumername="worker_01",
                streams=self.streaming_keys,
                count=500,
                block=5000
            )
            if response:
                for stream_name,messages in response:
                    print(f"📡 处理来自 {stream_name} 的 {len(messages)} 条消息")
                    s_key = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
                    if stream_name not in pending_ack:
                        pending_ack[s_key] = []

                    for msg_id,conetent in messages:
                        pending_ack[s_key].append(msg_id)
                        buffer.append((s_key,json.loads(conetent['data'])))

            if len(buffer) > self.batch_size or (time.time() - last_flush > self.flush_interval and buffer):
                success = await self._flush(buffer)
                if success:
                    acks = [
                        self.redis.xack(stream_name,self.group_name,*msg_ids)
                        for stream_name,msg_ids in pending_ack.items() if msg_ids
                    ]
                    if acks:
                        await asyncio.gather(*acks)

                    # 重置计数器和缓存
                    self.logger.info(f"✅ [ACK] Confirmed {len(buffer)} messages across {len(pending_ack)} streams.")
                    buffer = []
                    pending_ack = {}
                    last_flush = time.time()

    async def _flush(self,data):
        if not data: return

        buckets = {}
        for stream_key,content in data:
            parts = stream_key.split(':')
            if len(parts) < 5: continue

            mkt_type = parts[2]
            table_name = parts[-1]

            target_table = f"{table_name}_{mkt_type}"
            if target_table not in buckets:
                buckets[target_table] = []

            buckets[target_table].append(content)
        try:
            tasks = [
                self._insert_db(target_table,data)
                for target_table,data in buckets.items() if data
            ]
            await asyncio.gather(*tasks)
            return True
        except Exception as e:
            return False
            
    async def _insert_db(self,table,data):
        if not data: return

        start_time = time.time()
        df = pl.DataFrame(data)

        parquet_write_bytes.labels(table=table).inc(len(df))

        with parquet_write_duration.labels(table=table).time():
            try:
                arrow_table = df.to_arrow()
                self.ch.insert_arrow(table=table,arrow_table=arrow_table)
                duration = time.time()-start_time
                self.logger.info(f"🚢 [FLUSH] Table: {table} | Rows: {len(df)} | Latency: {duration:.3f}s")

                if duration > self.flush_interval * 0.8:
                    self.logger.warning(f"⚠️ [PRESSURE] DB write latency is nearing limit for {table}!")

            except Exception as e:
                self.logger.error(f"🔥 [DB-CRITICAL] Failed to insert into {table}: {e}")
                raise e
            
    async def system_monitor_task(self):
        while True:
            try:
                mem_info = await self.redis.info('memory')

                redis_mem_gauge.labels(type='used_bytes').set(mem_info['used_memory'])
                redis_mem_gauge.labels(type='fragmentation').set(mem_info['mem_fragmentation_ratio'])

                if mem_info['used_memory'] > 2.5 * 1024 * 1024 * 1024:
                    self.logger.critical("🚨 [MEM-CRITICAL] Redis memory > 2.5GB! System at risk.")
                    if self.streaming_keys:
                        s_keys = self.streaming_keys.keys()
                        for s_key in s_keys:
                            await self.redis.xtrim(s_key,maxlen=2000,approximate=True)

                    self.logger.warning("🧹 [TRIMMED] Emergency XTRIM completed for all streams.")

                await asyncio.sleep(10)

            except Exception as e:
                self.logger.error(f"Monitoring error: {e}")
                await asyncio.sleep(10)

    async def main(self):
        print(await self.redis.smembers('registry:streams:orderbook'))
        tasks = []
        tasks.append(asyncio.create_task(self.system_monitor_task()))
        tasks.append(asyncio.create_task(start_metrics_pusher(job_name="worker_syncer")))
        tasks.append(asyncio.create_task(self._get_redis_streaming_key()))
        tasks.append(asyncio.create_task(self.storage_worker()))

        await asyncio.gather(*tasks)

if __name__=='__main__':
    syncer = Syncer()
    asyncio.run(syncer.main())
        
daily_patcher.py:
import polars as pl
import os
import io
import gc
import requests
import time
import zipfile
from datetime import datetime,timedelta,timezone
from src.storage.clickhouse.client import ch_manager
from src.utils.logger import setup_logger

class DailyPatcher:
    """
    Automated Data Integrity & Reconciliation Engine.
    Compares local ClickHouse records against official exchange historical archives.
    Identifies sequence gaps and 'patches' missing trades to ensure 100% data fidelity.
    """
    def __init__(self,target_date:str):
        # Defaults to yesterday if no date is provided
        self.date_str = target_date or (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        self.logger = setup_logger(
            name='daily.patcher',
            log_file='logs/workers/daily_patcher.log'
        )
        self.logger.propagate = False
        self.ch_client = None
        self.exchange_ids = ['binance','okx']
        self.symbols = ['BTC/USDT','ETH/USDT']
        # Mapping raw exchange CSV headers to internal processing logic
        self.csv_columns = {
            'binance': ["trade_id","price","amount","cost","timestamp","is_maker","is_best"],
            'okx': ["symbol","trade_id","side","price","amount","timestamp"]
        }

    def setup(self):
        """Initializes database connectivity."""
        self.ch_client = ch_manager.connect

    def check_data_exists(self,target_date_str:str,exchange_id,symbol):
        """
        Performs a preliminary existence check. 
        If zero records exist for a day, triggers a full-day recovery.
        """
        sql = f"""
            SELECT 
                trade_id 
            FROM 
                trades_spot
            WHERE symbol='{symbol}' AND exchange_id='{exchange_id}'
                AND timestamp >= toUnixTimestamp64Milli(toDateTime64('{target_date_str} 00:00:00',3))
                AND timestamp < toUnixTimestamp64Milli(toDateTime64('{target_date_str} 00:00:00',3) + INTERVAL 1 DAYS)
            LIMIT 1
        """
        result = self.ch_client.query(sql)
        if not result.result_rows:
            self.logger.warning(f"🚨 [CRITICAL MISS] No data for {exchange_id} {symbol} on {target_date_str}. Triggering full recovery.")
            file_path = self.download_and_unzip(exchange_id,symbol,target_date_str)
            if file_path and os.path.exists(file_path):
                try:
                    official_df = self._changeColumns(exchange_id,symbol,file_path)
                    self.sync_to_clickhouse(official_df)
                    self.logger.info(f"✅ [RECOVERED] Successfully patched {len(official_df)} records.")

                    del official_df
                    return True
                except Exception as e:
                    self.logger.error(f"❌ [PATCH-ERROR] Failed to recover {exchange_id} {symbol}: {e}")
                finally:
                    if os.path.exists(file_path): os.remove(file_path)

        return False

    def main(self):
        """
        Orchestrates the reconciliation workflow:
        1. Fetch Official CSV -> 2. Query Local CH -> 3. Anti-Join to find gaps -> 4. Patch.
        """
        self.setup()

        for exchange_id in self.exchange_ids:
            for symbol in self.symbols:
                # Step 1: Handle complete outages
                flag = self.check_data_exists(self.date_str,exchange_id,symbol)
                gc.collect()
                if flag: continue

                # Step 2: Handle partial gaps
                file_path = self.download_and_unzip(exchange_id,symbol,self.date_str)
                if file_path and os.path.exists(file_path):
                    official_df = self._changeColumns(exchange_id,symbol,file_path)

                    max_trade_id = official_df['trade_id'].max()
                    min_trade_id = official_df['trade_id'].min()

                    ch_df = self.get_ch_data(self.date_str,exchange_id,symbol,max_trade_id,min_trade_id)

                    # --- CORE ALGORITHM: Anti-Join ---
                    # Returns rows in official_df that are NOT present in ch_df based on trade_id
                    gaps_df = official_df.join(ch_df,on="trade_id",how='anti')
                    
                    if not gaps_df.is_empty():
                        try:
                            self.sync_to_clickhouse(gaps_df)
                            self.logger.info(f"✅ [PATCHED] Injected {len(gaps_df)} missing records into {exchange_id} {symbol}.")
                            time.sleep(1)
                                
                        except Exception as e:
                            self.logger.error(f"❌ [GAP-ERROR] Patch failed: {e}")
                            
                    # Step 3: Final Verification (Full Reconciliation)
                    self.verify_full_integrity(exchange_id=exchange_id,symbol=symbol,official_df=official_df,file_path=file_path,max_trade_id=max_trade_id,min_trade_id=min_trade_id)

                gc.collect()

    def get_ch_data(self,target_date_str:str,exchange_id,symbol,max_trade_id,min_trade_id):
        """Fetches local sequence IDs for gap analysis."""

        sql = f"""
            SELECT
                trade_id
            FROM 
                trades_spot
            WHERE trade_id BETWEEN {min_trade_id} AND {max_trade_id}
            AND exchange_id='{exchange_id}' AND symbol='{symbol}'
            ORDER BY trade_id ASC
        """

        df = pl.from_pandas(self.ch_client.query_df(sql))
        if df.is_empty():
            return pl.DataFrame({"trade_id": []}, schema={"trade_id": pl.Int64})
        
        return df

    def download_and_unzip(self,exchange_id,symbol:str,date_obj):
        """Retrieves historical archives from official exchange CDN."""

        url,file_path = self._get_url(exchange_id,symbol,date_obj)
        os.makedirs(os.path.dirname(file_path),exist_ok=True)

        if os.path.exists(file_path):
            self.logger.info(f"file is exist {file_path}")
            return file_path
        
        self.logger.info(f"🌐 [FETCHING] Requesting official archive: {url}")
        start_time = time.time()
      
        try:
            r = requests.get(url,timeout=20)
            if r.status_code == 200:
                z = zipfile.ZipFile(io.BytesIO(r.content))
                z.extractall(f"temp/{exchange_id}/")
                return file_path
            
            return None
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ [DOWNLOAD-ERROR] {e}")
            return None
        
        except IOError as e:
            self.logger.error(f"❌ Disk failure: Unable to write data | Detail: {e}")
            return None

    def _get_url(self,exchange_id,symbol:str,date_obj:str):
        """Constructs CDN URLs for Binance Vision and OKX Static."""
        if exchange_id == 'binance':
            symbol = symbol.replace('/','').replace('-','')
            url = f"https://data.{exchange_id}.vision/data/spot/daily/trades/{symbol}/{symbol}-trades-{date_obj}.zip"
        elif exchange_id == 'okx':
            symbol = symbol.replace('/','-')
            clear_date = date_obj.replace('-','')
            url = f"https://static.okx.com/cdn/okex/traderecords/trades/daily/{clear_date}/{symbol}-trades-{date_obj}.zip"

        file_path = f"temp/{exchange_id}/{symbol}-trades-{date_obj}.csv"
        return url,file_path

    def _changeColumns(self,exchange_id,symbol,file_path:str):
        """Normalizes heterogeneous CSV formats into the unified project schema using Polars."""
        schema = ['trade_id','trade_id_raw','exchange_id','symbol','mkt_type','price','amount','timestamp','side','is_taker_buyer','local_timestamp']
        if exchange_id == 'binance':
            df = pl.scan_csv(file_path,has_header=False,new_columns=self.csv_columns[exchange_id]).collect()
            return df.with_columns([
                pl.lit(exchange_id).alias('exchange_id'),
                pl.lit(symbol).alias('symbol'),
                pl.lit('spot').alias('mkt_type'),
                pl.col('trade_id').cast(pl.Int64),
                pl.col('trade_id').cast(pl.String).alias('trade_id_raw'),
                pl.col('price').cast(pl.Float64),
                pl.col('amount').cast(pl.Float64),
                (pl.col("timestamp") // 1000).cast(pl.Int64).alias("timestamp"),
                pl.when(pl.col('is_maker') == False).then(pl.lit('buy')).otherwise(pl.lit('sell')).alias('side'),
                pl.col('is_maker').not_().alias('is_taker_buyer'),
                pl.lit(int(time.time() * 1000)).alias('local_timestamp')
            ]).select(schema)
        elif exchange_id == 'okx':
            df = pl.scan_csv(file_path).collect()
            return df.with_columns([
                pl.lit(exchange_id).alias('exchange_id'),
                pl.lit(symbol).alias('symbol'),
                pl.lit('spot').alias('mkt_type'),
                pl.col('trade_id').cast(pl.Int64),
                pl.col('trade_id').cast(pl.String).alias('trade_id_raw'),
                pl.col('price').cast(pl.Float64),
                pl.col('size').cast(pl.Float64).alias('amount'),
                pl.col("created_time").cast(pl.Int64).alias('timestamp'),
                pl.col('side'),
                (pl.col('side') == 'buy').alias('is_taker_buyer'),
                pl.lit(int(time.time() * 1000)).alias('local_timestamp')
            ]).select(schema)


    def sync_to_clickhouse(self,df:pl.DataFrame):
        """Performs batch insertion into ClickHouse."""
        arrow_table = df.to_arrow()
        self.logger.info(f"🚀 [SYNC] Pushing {len(df)} records to ClickHouse.")
        try:
            self.ch_client.insert_arrow(
                table='trades_spot',
                arrow_table=arrow_table
            )
        except Exception as e:
            self.logger.error(f"🚨 [DB-ERROR] Insertion failed: {e}")
            raise

    def verify_full_integrity(self,exchange_id,symbol,official_df:pl.DataFrame,file_path,max_trade_id,min_trade_id):
        """
        The 'Gold Standard' Check.
        Compares record counts and individual trade attributes (price/amount) to guarantee 100% precision.
        """
        try:
            self.logger.info(f"🔍 [AUDIT] Running full reconciliation: {exchange_id}-{symbol}")
            csv_df = official_df.with_columns([
                pl.col('price').round(8),
                pl.col('amount').round(8)
            ])

            sql = f"""
                SELECT
                    trade_id,
                    round(price,8) as price,
                    round(amount,8) as amount,
                    timestamp,
                    side,
                    is_taker_buyer
                FROM
                    trades_spot FINAL
                WHERE exchange_id='{exchange_id}' AND symbol='{symbol}'
                    AND trade_id BETWEEN {min_trade_id} AND {max_trade_id}
                ORDER BY trade_id ASC
            """
            ch_df = self.ch_client.query_df(sql)
            ch_df = pl.from_pandas(ch_df)

            diff = csv_df.join(ch_df,on='trade_id',how='anti')

            if diff.is_empty() and len(csv_df) == len(ch_df):
                self.logger.info(f"💎 [AUDIT-PASSED] 100% Data Integrity for {exchange_id} {symbol}.")
                if os.path.exists(file_path):
                    os.remove(file_path)
                return True
            else:
                self.logger.error(f"🚨 [AUDIT-FAILED] Mismatch detected! Gaps found: {len(diff)}")
                return False

        except Exception as e:
            self.logger.error(f"🚨 [AUDIT-CRASH] Audit process failed: {e}")
            return False

def patcher(target_date=None):
    instance = DailyPatcher(target_date=target_date)
    instance.main()

if __name__ == '__main__':
    patcher()

consolidator.py:
from src.storage.clickhouse.client import ch_manager
from src.utils.logger import setup_logger
import os
import gc
from datetime import datetime,timedelta,timezone


class Consolidator:
    """
    Data ETL & Feature Engineering Engine.
    Converts raw ClickHouse records into optimized Parquet files while 
    calculating core alpha features for quantitative research.
    """
    def __init__(self,target_date:str=None):
        self.ch_client = None
        self.logger = setup_logger("workers.consolidator")
        self.target_date = target_date
        self.exchanges = ['binance','okx']
        self.symbols = ['BTC/USDT','ETH/USDT']
        self.data_types = ['orderbook','trades']
        self.mkt_types = ['spot']
        self.fields = {
            'orderbook':"""
                    nonce,
                    symbol,
                    mkt_type,
                    exchange_id,
                    fromUnixTimestamp64Milli(timestamp,'UTC') AS dt,
                    (bid_prices[1] * ask_volumes[1] + ask_prices[1] * bid_volumes[1]) / nullIf(bid_volumes[1] + ask_volumes[1],0) AS micro_price,
                    (bid_volumes[1] - ask_volumes[1]) / nullIf(bid_volumes[1] + ask_volumes[1],0) AS imbalance,
                    ask_prices[1] - bid_prices[1] AS spread,
                    arraySlice(bid_prices,1,20) AS bid_prices,
                    arraySlice(bid_volumes,1,20) AS bid_volumes,
                    arraySlice(ask_prices,1,20) AS ask_prices,
                    arraySlice(ask_volumes,1,20) AS ask_volumes,
                    timestamp""",
            'trades':"""
                trade_id,
                trade_id_raw,
                symbol,
                mkt_type,
                exchange_id,
                fromUnixTimestamp64Milli(timestamp,'UTC') AS dt,
                price,
                amount,
                price * amount AS turnover,
                side,
                is_taker_buyer,
                row_number() OVER (ORDER BY trade_id) AS sub_ms_seq,
                avg(price) OVER (ORDER BY trade_id ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) as ma_price_100,
                if(price * amount > 50000, 1, 0) as is_high_impact,
                timestamp
            """
        }
        self.sort_keys = {
            'orderbook': 'nonce',
            'trades': 'trade_id'
        }

    def setup(self):
        """Initializes database connection."""
        self.ch_client = ch_manager.connect

    def daily_feature_consolidation(self,symbol:str,exchange_id:str,mkt_type:str,data_type:str,current_date:str):
        """
        Materializes daily raw data into professional Parquet format for backtesting.
        """
        clear_symbol = symbol.replace('/','-')
        table_name = f"market_data.{data_type}_spot"
        target_date_obj = datetime.strptime(current_date,'%Y-%m-%d')

        # Standardized hierarchical storage structure
        dir_path = os.path.join(
            "data/processed",
            exchange_id,
            mkt_type,
            clear_symbol,
            data_type
        )
        os.makedirs(dir_path,exist_ok=True)
        file_path = os.path.join(
            dir_path,
            f"{target_date_obj.strftime('%Y%m%d')}.parquet"
        )

        # Cleanup corrupt/empty files from previous runs
        if os.path.exists(file_path):
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            if size_mb < 1:
                os.remove(file_path)

        if not os.path.exists(file_path):
            start_ms = int(target_date_obj.replace(tzinfo=timezone.utc).timestamp() * 1000)
            end_ms = start_ms + (24 * 60 * 60 * 1000) - 1

            # Leveraging ClickHouse S3/File integration for high-speed export
            sql = f"""
                INSERT INTO FUNCTION file('{file_path}','Parquet')
                SELECT
                    {self.fields[data_type]}
                FROM {table_name} FINAL
                WHERE symbol='{symbol}' 
                    AND exchange_id='{exchange_id}' 
                    AND mkt_type='{mkt_type}'
                    AND timestamp >= {start_ms}
                    AND timestamp <= {end_ms}
                ORDER BY {self.sort_keys[data_type]} ASC
            """
            try:
                self.logger.info(f"📊 Consolidating features: {exchange_id} {symbol} @ {current_date}")
                self.ch_client.command(sql)
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                self.logger.info(f"✨ Export successful: {file_path} | Size: {size_mb:.2f}MB")
            except Exception as e:
                self.logger.error(f"❌ Export failed: {e}")
                raise

        gc.collect() # Explicit garbage collection to manage large memory frames

    def run(self):
        """Main execution flow for daily ETL."""
        self.setup()

        is_automated = self.target_date is None

        for exchange_id in self.exchanges:
            # Handle time-zone offsets for different exchanges
            if is_automated:
                days_offset = 2 if exchange_id == 'okx' else 1
                current_date = (datetime.now(timezone.utc) - timedelta(days=days_offset)).strftime('%Y-%m-%d')
            else:
                current_date = self.target_date

            for mkt_type in self.mkt_types:
                for symbol in self.symbols:
                    for data_type in self.data_types:
                        self.daily_feature_consolidation(
                            symbol=symbol,
                            exchange_id=exchange_id,
                            mkt_type=mkt_type,
                            data_type=data_type,
                            current_date=current_date
                        )

def consolidator(target_date: str=None):
    """EntryPoint for Task Scheduler."""
    consolidator_obj = Consolidator(target_date)
    consolidator_obj.run()

if __name__ == '__main__':
    consolidator()
                            
