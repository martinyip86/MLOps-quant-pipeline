from src.storage.clickhouse.client import ch_manager
from src.utils.logger import setup_logger
from src.workers.feature_processor import FeatureProcessor
import polars as pl
import os
import gc
import time
from datetime import datetime,timedelta,timezone

class Consolidator:
    def __init__(self):
        self.logger = setup_logger("workers.consolidator")
        self.exchange_ids = ['binance']
        self.symbols = ['BTC/USDT','ETH/USDT','SOL/USDT','XRP/USDT']
        self.ch = ch_manager.connect('hk')
        self.processed_path = "data/processed"

    def _generate_filepath(self,exchange_id:str,symbol:str,mkt_type:str,watch_type:str,target_date:str):
        clear_symbol = symbol.replace('/','-')
        file_path = os.path.join(
            self.processed_path,
            exchange_id,
            mkt_type,
            clear_symbol,
            watch_type,
            f"{target_date.replace('-','')}.parquet"
        )
        os.makedirs(os.path.dirname(file_path),exist_ok=True)
        return file_path


    def _export_orderbook_spot(self,exchange_id:str,symbol:str,target_date:str):
        file_path = self._generate_filepath(exchange_id,symbol,'spot','orderbook',target_date)
        if not os.path.exists(file_path):
            date_obj = datetime.strptime(target_date,'%Y-%m-%d')
            date_obj = date_obj.replace(tzinfo=timezone.utc)
            interval_ms = 4 * 60 * 60 * 1000

            settings = {
                'max_threads': 1,               # 必须为1，严禁并发
                'max_block_size': 500,         # 极其重要：从 8192 降到 1000，减小服务器单次读取的负担
                'max_memory_usage': '1G',       # 限制服务器使用的总内存
                'preferred_block_size_bytes': '1048576',
            }
            column_names = [
                'bid_prices', 
                'bid_volumes', 
                'ask_prices', 
                'ask_volumes', 
                'timestamp'
            ]
            chunks = []

            for i in range(6):
                start_ts = int(date_obj.timestamp() * 1000 + i * interval_ms)
                end_ts = start_ts + interval_ms -1
                sql = f"""
                    SELECT 
                        bid_prices,
                        bid_volumes,
                        ask_prices,
                        ask_volumes,
                        timestamp
                    FROM market_data.orderbook_spot
                    WHERE exchange_id='{exchange_id}'
                        AND symbol='{symbol}'
                        AND timestamp >= {start_ts}
                        AND timestamp <= {end_ts}
                """
                
                with self.ch.query_column_block_stream(sql,settings=settings) as stream:
                    for block in stream:
                        if not block: continue
                        chunk_df = pl.from_dict(dict(zip(column_names,block)))
                        chunks.append(chunk_df)

                gc.collect()

            if chunks:
                df:pl.DataFrame = pl.concat(chunks,rechunk=True)
                del chunks
                df = df.with_columns([
                    pl.lit(exchange_id).alias('exchange_id'),
                    pl.lit(symbol).alias('symbol'),
                    pl.lit('spot').alias('mkt_type'),
                    ((pl.col('bid_prices').list.get(0) * pl.col('ask_volumes').list.get(0) + pl.col('ask_prices').list.get(0) * pl.col('bid_volumes').list.get(0)) / (pl.col('bid_volumes').list.get(0) + pl.col('ask_volumes').list.get(0) + 1e-8)).alias('micro_price'),
                    (pl.col('ask_prices').list.get(0) - pl.col('bid_prices').list.get(0)).alias('spread'),
                    ((pl.col('bid_prices').list.get(0) + pl.col('ask_prices').list.get(0)) / 2).alias('mid_price'),
                    ((pl.col('ask_prices').list.slice(0,20) * pl.col('ask_volumes').list.slice(0,20)).list.sum() / (pl.col('ask_volumes').list.slice(0,20).list.sum() + 1e-8)).alias('sim_buy_price_avg')
                ]).with_columns([
                    ((pl.col('sim_buy_price_avg') / pl.col('mid_price') - 1) * 10000).alias('buy_impact_bps')
                ]).sort('timestamp')
                tmp_path = f"{file_path}.tmp"
                df.write_parquet(tmp_path)
                os.replace(tmp_path,file_path)
                del df
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                self.logger.info(f"✨ Export successful: {file_path} | Size: {size_mb:.2f}MB")

    def _export_orderbook_future(self,exchange_id:str,symbol:str,target_date:str):
        file_path = self._generate_filepath(exchange_id,symbol,'future','orderbook',target_date)
        if not os.path.exists(file_path):
            date_obj = datetime.strptime(target_date,'%Y-%m-%d')
            date_obj = date_obj.replace(tzinfo=timezone.utc)
            interval_ms = 4 * 60 * 60 * 1000

            settings = {
                'max_threads': 1,               # 必须为1，严禁并发
                'max_block_size': 500,         # 极其重要：从 8192 降到 1000，减小服务器单次读取的负担
                'max_memory_usage': '1G',       # 限制服务器使用的总内存
                'preferred_block_size_bytes': '1048576',
            }
            column_names = [
                'bid_prices', 
                'bid_volumes', 
                'ask_prices', 
                'ask_volumes', 
                'timestamp'
            ]
            chunks = []

            for i in range(6):
                start_ts = int(date_obj.timestamp() * 1000 + i * interval_ms)
                end_ts = start_ts + interval_ms -1
                sql = f"""
                    SELECT 
                        bid_prices,
                        bid_volumes,
                        ask_prices,
                        ask_volumes,
                        timestamp
                    FROM market_data.orderbook_future
                    WHERE exchange_id='{exchange_id}'
                        AND symbol='{symbol}'
                        AND timestamp >= {start_ts}
                        AND timestamp <= {end_ts}
                """
                
                with self.ch.query_column_block_stream(sql,settings=settings) as stream:
                    for block in stream:
                        if not block: continue
                        chunk_df = pl.from_dict(dict(zip(column_names,block)))
                        chunks.append(chunk_df)

                gc.collect()

            if chunks:
                df:pl.DataFrame = pl.concat(chunks,rechunk=True)
                del chunks
                df = df.with_columns([
                    pl.lit(exchange_id).alias('exchange_id'),
                    pl.lit(symbol).alias('symbol'),
                    pl.lit('future').alias('mkt_type'),
                    ((pl.col('bid_prices').list.get(0) * pl.col('ask_volumes').list.get(0) + pl.col('ask_prices').list.get(0) * pl.col('bid_volumes').list.get(0)) / (pl.col('bid_volumes').list.get(0) + pl.col('ask_volumes').list.get(0) + 1e-8)).alias('micro_price'),
                    (pl.col('ask_prices').list.get(0) - pl.col('bid_prices').list.get(0)).alias('spread'),
                    ((pl.col('bid_prices').list.get(0) + pl.col('ask_prices').list.get(0)) / 2).alias('mid_price'),
                    ((pl.col('ask_prices').list.slice(0,20) * pl.col('ask_volumes').list.slice(0,20)).list.sum() / (pl.col('ask_volumes').list.slice(0,20).list.sum() + 1e-8)).alias('sim_buy_price_avg')
                ]).with_columns([
                    ((pl.col('sim_buy_price_avg') / pl.col('mid_price') - 1) * 10000).alias('buy_impact_bps')
                ]).sort('timestamp')
                tmp_path = f"{file_path}.tmp"
                df.write_parquet(tmp_path)
                os.replace(tmp_path,file_path)
                del df
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                self.logger.info(f"✨ Export successful: {file_path} | Size: {size_mb:.2f}MB")

    def _export_trades_spot(self,exchange_id:str,symbol:str,target_date:str):
        file_path = self._generate_filepath(exchange_id,symbol,'spot','trades',target_date)
        if not os.path.exists(file_path):
            date_obj = datetime.strptime(target_date,'%Y-%m-%d')
            date_obj = date_obj.replace(tzinfo=timezone.utc)
            start_ts = int(date_obj.timestamp() * 1000)
            end_ts = start_ts + 24 * 60 * 60 * 1000 -1
            sql = f"""
                SELECT
                    trade_id,
                    price,
                    amount,
                    side,
                    is_taker_buyer,
                    timestamp
                FROM market_data.trades_spot
                WHERE exchange_id='{exchange_id}'
                    AND symbol='{symbol}'
                    AND price > 0
                    AND amount > 0
                    AND timestamp >= {start_ts}
                    AND timestamp <= {end_ts}
            """
            settings = {
                'max_threads': 1,               # 必须为1，严禁并发
                'max_block_size': 500,         # 极其重要：从 8192 降到 1000，减小服务器单次读取的负担
                'max_memory_usage': '1G',       # 限制服务器使用的总内存
                'preferred_block_size_bytes': '1048576',
            }
            column_names = ['trade_id','price','amount','side','is_taker_buyer','timestamp']
            chunks = []
            with self.ch.query_column_block_stream(sql,settings=settings) as stream:
                for block in stream:
                    if not block: continue
                    chunk_df = pl.from_dict(dict(zip(column_names,block)))
                    chunks.append(chunk_df)
            if chunks:
                df:pl.DataFrame = pl.concat(chunks,rechunk=True)
                del chunks
                df = df.with_columns([
                    pl.lit(exchange_id).alias('exchange_id'),
                    pl.lit(symbol).alias('symbol'),
                    pl.lit('spot').alias('mkt_type')
                ]).sort('timestamp')
                tmp_path = f"{file_path}.tmp"
                df.write_parquet(tmp_path)
                os.replace(tmp_path,file_path)
                del df
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                self.logger.info(f"✨ Export successful: {file_path} | Size: {size_mb:.2f}MB")

    def _export_trades_future(self,exchange_id:str,symbol:str,target_date:str):
        file_path = self._generate_filepath(exchange_id,symbol,'future','trades',target_date)
        if not os.path.exists(file_path):
            date_obj = datetime.strptime(target_date,'%Y-%m-%d')
            date_obj = date_obj.replace(tzinfo=timezone.utc)
            start_ts = int(date_obj.timestamp() * 1000)
            end_ts = start_ts + 24 * 60 * 60 * 1000 -1
            sql = f"""
                SELECT
                    trade_id,
                    price,
                    amount,
                    side,
                    timestamp
                FROM market_data.trades_future
                WHERE exchange_id='{exchange_id}'
                    AND symbol='{symbol}'
                    AND price > 0
                    AND amount > 0
                    AND timestamp >= {start_ts}
                    AND timestamp <= {end_ts}
            """

            settings = {
                'max_threads': 1,               # 必须为1，严禁并发
                'max_block_size': 500,         # 极其重要：从 8192 降到 1000，减小服务器单次读取的负担
                'max_memory_usage': '1G',       # 限制服务器使用的总内存
                'preferred_block_size_bytes': '1048576',
            }
            column_names = ['trade_id','price','amount','side','timestamp']
            chunks = []
            with self.ch.query_column_block_stream(sql,settings=settings) as stream:
                for block in stream:
                    if not block: continue
                    chunk_df = pl.from_dict(dict(zip(column_names,block)))
                    chunks.append(chunk_df)
            if chunks:
                df:pl.DataFrame = pl.concat(chunks,rechunk=True)
                del chunks
                df = df.with_columns([
                    pl.lit(exchange_id).alias('exchange_id'),
                    pl.lit(symbol).alias('symbol'),
                    pl.lit('future').alias('mkt_type')
                ]).sort('timestamp')
                tmp_path = f"{file_path}.tmp"
                df.write_parquet(tmp_path)
                os.replace(tmp_path,file_path)
                del df
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                self.logger.info(f"✨ Export successful: {file_path} | Size: {size_mb:.2f}MB")

    def _export_mark_price_future(self,exchange_id:str,symbol:str,target_date:str):
        file_path = self._generate_filepath(exchange_id,symbol,'future','mark_price',target_date)
        if not os.path.exists(file_path):
            date_obj = datetime.strptime(target_date,'%Y-%m-%d')
            date_obj = date_obj.replace(tzinfo=timezone.utc)
            start_ts = int(date_obj.timestamp() * 1000)
            end_ts = start_ts + 24 * 60 * 60 * 1000 -1
            sql = f"""
                SELECT
                    mark_price,
                    index_price,
                    timestamp
                FROM market_data.market_price_future
                WHERE exchange_id='{exchange_id}'
                    AND symbol='{symbol}'
                    AND timestamp >= {start_ts}
                    AND timestamp <= {end_ts}
            """
            settings = {
                'max_threads': 1,               # 必须为1，严禁并发
                'max_block_size': 500,         # 极其重要：从 8192 降到 1000，减小服务器单次读取的负担
                'max_memory_usage': '1G',       # 限制服务器使用的总内存
                'preferred_block_size_bytes': '1048576',
            }
            column_names = ['mark_price','index_price','timestamp']
            chunks = []
            with self.ch.query_column_block_stream(sql,settings=settings) as stream:
                for block in stream:
                    if not block: continue
                    chunk_df = pl.from_dict(dict(zip(column_names,block)))
                    chunks.append(chunk_df)
            if chunks:
                df:pl.DataFrame = pl.concat(chunks,rechunk=True)
                del chunks
                df = df.with_columns([
                    pl.lit(exchange_id).alias('exchange_id'),
                    pl.lit(symbol).alias('symbol'),
                    pl.lit('future').alias('mkt_type')
                ]).sort('timestamp')
                tmp_path = f"{file_path}.tmp"
                df.write_parquet(tmp_path)
                os.replace(tmp_path,file_path)
                del df
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                self.logger.info(f"✨ Export successful: {file_path} | Size: {size_mb:.2f}MB")

    def _export_open_interest_future(self,exchange_id:str,symbol:str,target_date:str):
        file_path = self._generate_filepath(exchange_id,symbol,'future','open_interest',target_date)
        if not os.path.exists(file_path):
            date_obj = datetime.strptime(target_date,'%Y-%m-%d')
            date_obj = date_obj.replace(tzinfo=timezone.utc)
            start_ts = int(date_obj.timestamp() * 1000)
            end_ts = start_ts + 24 * 60 * 60 * 1000 -1
            sql = f"""
                SELECT
                    base_volume,
                    open_interest_amount,
                    timestamp
                FROM market_data.open_interest_future
                WHERE exchange_id='{exchange_id}'
                    AND symbol='{symbol}'
                    AND timestamp >= {start_ts}
                    AND timestamp <= {end_ts}
            """
            settings = {
                'max_threads': 1,               # 必须为1，严禁并发
                'max_block_size': 500,         # 极其重要：从 8192 降到 1000，减小服务器单次读取的负担
                'max_memory_usage': '1G',       # 限制服务器使用的总内存
                'preferred_block_size_bytes': '1048576',
            }
            column_names = ['base_volume','open_interest_amount','timestamp']
            chunks = []
            with self.ch.query_column_block_stream(sql,settings=settings) as stream:
                for block in stream:
                    if not block: continue
                    chunk_df = pl.from_dict(dict(zip(column_names,block)))
                    chunks.append(chunk_df)
            if chunks:
                df:pl.DataFrame = pl.concat(chunks,rechunk=True)
                del chunks
                df = df.with_columns([
                    pl.lit(exchange_id).alias('exchange_id'),
                    pl.lit(symbol).alias('symbol'),
                    pl.lit('future').alias('mkt_type')
                ]).sort('timestamp')
                tmp_path = f"{file_path}.tmp"
                df.write_parquet(tmp_path)
                os.replace(tmp_path,file_path)
                del df
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                self.logger.info(f"✨ Export successful: {file_path} | Size: {size_mb:.2f}MB")

    def _export_funding_rate_future(self,exchange_id:str,symbol:str,target_date:str):
        file_path = self._generate_filepath(exchange_id,symbol,'future','funding_rate',target_date)
        if not os.path.exists(file_path):
            date_obj = datetime.strptime(target_date,'%Y-%m-%d')
            date_obj = date_obj.replace(tzinfo=timezone.utc)
            start_ts = int(date_obj.timestamp() * 1000)
            end_ts = start_ts + 24 * 60 * 60 * 1000 -1
            sql = f"""
                SELECT
                    funding_rate,
                    next_funding_rate_timestamp,
                    timestamp
                FROM market_data.funding_rate_future
                WHERE exchange_id='{exchange_id}'
                    AND symbol='{symbol}'
                    AND timestamp >= {start_ts}
                    AND timestamp <= {end_ts}
            """
            settings = {
                'max_threads': 1,               # 必须为1，严禁并发
                'max_block_size': 500,         # 极其重要：从 8192 降到 1000，减小服务器单次读取的负担
                'max_memory_usage': '1G',       # 限制服务器使用的总内存
                'preferred_block_size_bytes': '1048576',
            }
            column_names = ['funding_rate','next_funding_rate_timestamp','timestamp']
            chunks = []
            with self.ch.query_column_block_stream(sql,settings=settings) as stream:
                for block in stream:
                    if not block: continue
                    chunk_df = pl.from_dict(dict(zip(column_names,block)))
                    chunks.append(chunk_df)
            if chunks:
                df:pl.DataFrame = pl.concat(chunks,rechunk=True)
                del chunks
                df = df.with_columns([
                    pl.lit(exchange_id).alias('exchange_id'),
                    pl.lit(symbol).alias('symbol'),
                    pl.lit('future').alias('mkt_type')
                ]).sort('timestamp')
                tmp_path = f"{file_path}.tmp"
                df.write_parquet(tmp_path)
                os.replace(tmp_path,file_path)
                del df
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                self.logger.info(f"✨ Export successful: {file_path} | Size: {size_mb:.2f}MB")

    def _export_liquidations_future(self,exchange_id:str,symbol:str,target_date:str):
        file_path = self._generate_filepath(exchange_id,symbol,'future','liquidations',target_date)
        if not os.path.exists(file_path):
            date_obj = datetime.strptime(target_date,'%Y-%m-%d')
            date_obj = date_obj.replace(tzinfo=timezone.utc)
            start_ts = int(date_obj.timestamp() * 1000)
            end_ts = start_ts + 24 * 60 * 60 * 1000 -1
            sql = f"""
                SELECT
                    price,
                    amount,
                    side,
                    time_in_force,
                    order_status,
                    timestamp
                FROM market_data.liquidations_future
                WHERE exchange_id='{exchange_id}'
                    AND symbol='{symbol}'
                    AND timestamp >= {start_ts}
                    AND timestamp <= {end_ts}
            """
            settings = {
                'max_threads': 1,               # 必须为1，严禁并发
                'max_block_size': 500,         # 极其重要：从 8192 降到 1000，减小服务器单次读取的负担
                'max_memory_usage': '1G',       # 限制服务器使用的总内存
                'preferred_block_size_bytes': '1048576',
            }
            column_names = ['price','amount','side','time_in_force','order_status','timestamp']
            chunks = []
            with self.ch.query_column_block_stream(sql,settings=settings) as stream:
                for block in stream:
                    if not block: continue
                    chunk_df = pl.from_dict(dict(zip(column_names,block)))
                    chunks.append(chunk_df)
            if chunks:
                df:pl.DataFrame = pl.concat(chunks,rechunk=True)
                del chunks
                df = df.with_columns([
                    pl.lit(exchange_id).alias('exchange_id'),
                    pl.lit(symbol).alias('symbol'),
                    pl.lit('future').alias('mkt_type')
                ]).sort('timestamp')
                tmp_path = f"{file_path}.tmp"
                df.write_parquet(tmp_path)
                os.replace(tmp_path,file_path)
                del df
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                self.logger.info(f"✨ Export successful: {file_path} | Size: {size_mb:.2f}MB")

    def main(self,target_date:str=None):
        self.logger.info("generate summary files are starting now...")
        for exchange_id in self.exchange_ids:
            if target_date is None or target_date >= datetime.now(timezone.utc).strftime('%Y-%m-%d'):
                if exchange_id == 'binance':
                    current_target_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
                elif exchange_id == 'okx':
                    current_target_date = (datetime.now(timezone.utc) - timedelta(days=2)).strftime('%Y-%m-%d')
            else:
                current_target_date = target_date

            print(current_target_date)
            for symbol in self.symbols:
                self._export_orderbook_spot(exchange_id,symbol,current_target_date)
                self._export_orderbook_future(exchange_id,symbol,current_target_date)
                self._export_trades_spot(exchange_id,symbol,current_target_date)
                self._export_trades_future(exchange_id,symbol,current_target_date)
                self._export_mark_price_future(exchange_id,symbol,current_target_date)
                self._export_open_interest_future(exchange_id,symbol,current_target_date)
                self._export_funding_rate_future(exchange_id,symbol,current_target_date)
                self._export_liquidations_future(exchange_id,symbol,current_target_date)

        self.logger.info("generate completed.")

if __name__ == '__main__':
    obj = Consolidator()
    obj.main()