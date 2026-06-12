from src.storage.clickhouse.client import ch_manager
from src.utils.logger import setup_logger
from datetime import datetime,timezone,timedelta
import polars as pl
import requests
import os
import zipfile
import io
import time

class BasePatcher:
    def __init__(self,exchange_id:str,symbol:str,target_date:str,logger):
        if target_date is None or target_date >= datetime.now(timezone.utc).strftime('%Y-%m-%d'):
            if exchange_id == 'binance':
                self.target_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
            elif exchange_id == 'okx':
                self.target_date = (datetime.now(timezone.utc) - timedelta(days=2)).strftime('%Y-%m-%d')
        else:
            self.target_date = target_date

        self.logger = logger
        self.exchange_id = exchange_id
        self.symbol = symbol
        self.ch = ch_manager.connect('HK_HOST')

    def main(self):
        pass

    def _download_csv(self,exchange_id:str,mkt_type:str,url:str,file_path:str):
        os.makedirs(os.path.dirname(file_path),exist_ok=True)

        if os.path.exists(file_path):
            return True
        
        try:
            r = requests.get(url,timeout=20)
            if r.status_code == 200:
                z = zipfile.ZipFile(io.BytesIO(r.content))
                z.extractall(f"temp/{exchange_id}/{mkt_type}")
                return True
            else:
                return False
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ [DOWNLOAD-ERROR] {e}")
            return False
        
        except IOError as e:
            self.logger.error(f"❌ Disk failure: Unable to write data | Detail: {e}")
            return False
        
    def export_parquet(self,df:pl.DataFrame,table:str):
        file_path = os.path.join(
            "data/patch",
            self.target_date,
            self.symbol.replace('/','-'),
            f"{table}.parquet"
        )
        os.makedirs(os.path.dirname(file_path),exist_ok=True)
        if not os.path.exists(file_path):
            tmp_path = f"{file_path}.tmp"
            df.write_parquet(tmp_path)
            os.replace(tmp_path,file_path)
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            self.logger.info(f"✨ Export successful: {file_path} | Size: {size_mb:.2f}MB")
        
    def sync_to_clickhouse(self,df:pl.DataFrame,table:str):
        chunk_size = 5000
        total_row  = len(df)

        for i in range(0,total_row,chunk_size):
            chunk = df.slice(i,chunk_size)
            self.logger.info(f"🚀 [SYNC][{self.exchange_id}][{self.symbol}][{table}] Pushing chunk {i//chunk_size + 1} ({len(chunk)} rows)")
            try:
                self.ch.insert_arrow(
                    table=table,
                    arrow_table=chunk.to_arrow(),
                    settings={
                        'async_insert': 0, 
                        # 'wait_for_async_insert': 1,
                        'max_insert_block_size': chunk_size,
                        "receive_timeout": 300,
                        "send_timeout": 300,
                        "connect_timeout": 30,
                    }
                )
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"🚨 [DB-ERROR] Insertion failed: {e}")
                try:
                    self.ch.close()
                except Exception:
                    pass
                self.ch = ch_manager.connect()
                raise