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
        self.logger = setup_logger('daily.patcher',log_file='logs/syncer/daily_patcher.log')
        self.logger.propagate = False
        self.ch_client = None
        self.exchange_ids = ['binance','okx']
        self.symbols = ['BTC/USDT','ETH/USDT','PEPE/USDT','SOL/USDT']
        # Mapping raw exchange CSV headers to internal processing logic
        self.csv_columns = {
            'binance': ["trade_id","price","amount","cost","timestamp","is_maker","is_best"],
            'okx': ["symbol","trade_id","side","price","amount","timestamp"]
        }

    def setup(self):
        """Initializes database connectivity."""
        self.ch_client = ch_manager.market_db

    def check_data_exists(self,target_date_str:str,exchange_id,symbol):
        """
        Performs a preliminary existence check. 
        If zero records exist for a day, triggers a full-day recovery.
        """
        sql = f"""
            SELECT 
                trade_id 
            FROM 
                trades 
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
                trades
            WHERE trade_id BETWEEN {min_trade_id} AND {max_trade_id}
            AND exchange_id='{exchange_id}' AND symbol='{symbol}'
            ORDER BY trade_id ASC
        """

        df = pl.from_pandas(self.ch_client.query_df(sql))
        if df.is_empty():
            return pl.DataFrame({"trade_id": []}, schema={"trade_id": pl.String})
        
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
                    trades FINAL
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