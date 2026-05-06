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
        self.ch = ch_manager.connect()
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
                for w_type in ['orderbook','trades','market_price','open_interest']:
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
        