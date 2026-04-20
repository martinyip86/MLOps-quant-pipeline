from datetime import datetime,timezone
import polars as pl
import glob
import os

class FeatureProcessor:
    def __init__(self):
        self.raw_base_path = 'data/raw'
        self.processed_path = 'data/processed'

    def process_daily_data(self,exchange_id:str,mkt_type:str,symbol:str,watch_type:str,date_str:str,logger):
        datetime_obj = datetime.strptime(date_str,'%Y-%m-%d')
        target_path = os.path.join(
            self.processed_path,
            exchange_id,
            mkt_type,
            symbol.replace('/','-'),
            watch_type,
            f"{date_str.replace('-','')}.parquet"
        )
        temp_path = f"{target_path}.tmp"
        if not os.path.exists(target_path):
            from_path = os.path.join(
                self.raw_base_path,
                exchange_id,
                mkt_type,
                symbol.replace('/','-'),
                watch_type,
                datetime_obj.strftime('%Y'),
                datetime_obj.strftime('%m'),
                datetime_obj.strftime('%d'),
                "*.parquet"
            )
            files = sorted(glob.glob(from_path))

            if files:
                logger.info(f"🔄 Processing {symbol} for {date_str}...")
                try:
                    df = pl.scan_parquet(files).sort('timestamp').collect()
                    df.write_parquet(temp_path,compression='snappy')
                    os.replace(temp_path,target_path)
                    size_mb = os.path.getsize(target_path) / (1024 * 1024)
                    logger.info(f"✅ Processed file saved: {target_path} | Size: {size_mb:.2f}MB")
                    self._cleanup_raw_files(files)
                except Exception as e:
                    logger.error(f"❌ Processing failed: {e}")

    def _cleanup_raw_files(self,file_list):
        for f in file_list:
            try:
                os.remove(f)
            except Exception as e:
                print(f"Failed to delete {f}: {e}")