from src.storage.clickhouse.client import ch_manager
from src.utils.logger import setup_logger
from src.workers.feature_processor import FeatureProcessor
import polars as pl
import os
import gc
import time
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
                    (bid_prices[1] + ask_prices[1]) / 2 as mid_price,
                    (
                        arraySum(
                            arrayMap(
                                (p,v) -> p * v,
                                arraySlice(ask_prices,1,20),
                                arraySlice(ask_volumes,1,20)
                            )
                        ) /
                        nullIf(arraySum(arraySlice(ask_volumes,1,20)),0)
                    ) AS sim_buy_price_avg,
                    ((sim_buy_price_avg / mid_price) - 1) * 10000 AS buy_impact_bps, 
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
        self.ch_client = ch_manager.connect('hk')

    def daily_feature_consolidation(self,symbol:str,exchange_id:str,mkt_type:str,data_type:str,current_date:str):
        """
        Materializes daily raw data into professional Parquet format for backtesting.
        """
        clear_symbol = symbol.replace('/','-')
        table_name = f"market_data.{data_type}_spot"
        target_date_obj = datetime.strptime(current_date,'%Y-%m-%d')

        # Standardized hierarchical storage structure
        dir_path = os.path.join(
            "data/raw",
            exchange_id,
            mkt_type,
            clear_symbol,
            data_type,
            target_date_obj.strftime('%Y'),
            target_date_obj.strftime('%m'),
            target_date_obj.strftime('%d'),
        )
        os.makedirs(dir_path,exist_ok=True)
        
        start_ms = int(target_date_obj.replace(tzinfo=timezone.utc).timestamp() * 1000)
        # end_ms = start_ms + (24 * 60 * 60 * 1000) - 1
        for hour in range(24):
            h_start = start_ms + (hour * 3600 * 1000)
            h_end = h_start + (3600 * 1000) - 1

            file_path = os.path.join(
                dir_path,
                f"{target_date_obj.strftime('%Y%m%d')}_h{hour:02d}.parquet"
            )

            # Cleanup corrupt/empty files from previous runs
            if os.path.exists(file_path):
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                if size_mb < 1:
                    os.remove(file_path)

            if not os.path.exists(file_path):
                # Leveraging ClickHouse S3/File integration for high-speed export
                sql = f"""
                    SELECT
                        {self.fields[data_type]}
                    FROM {table_name}
                    WHERE symbol='{symbol}' 
                        AND exchange_id='{exchange_id}' 
                        AND mkt_type='{mkt_type}'
                        AND timestamp >= {h_start}
                        AND timestamp <= {h_end}
                    ORDER BY {self.sort_keys[data_type]} ASC
                """
                try:
                    self.logger.info(f"📊 Consolidating features: {exchange_id} {symbol} @ {current_date}")
                    settings = {
                        'max_memory_usage': 2000000000,          # 限制每個 query 用 2GB
                        'max_bytes_before_external_group_by': 1000000000, # 唔夠 RAM 就寫入臨時 Disk
                        'max_bytes_before_external_sort': 1000000000,     # 唔夠 RAM 就寫入臨時 Disk
                    }
                    table_arrow = self.ch_client.query_arrow(sql,settings=settings)
                    df = pl.from_arrow(table_arrow)
                    df.write_parquet(file_path)
                    del df
                    size_mb = os.path.getsize(file_path) / (1024 * 1024)
                    self.logger.info(f"✨ Export successful: {file_path} | Size: {size_mb:.2f}MB")
                    time.sleep(2)
                except Exception as e:
                    self.logger.error(f"❌ Export failed: {e}")
                    raise

            gc.collect() # Explicit garbage collection to manage large memory frames

    def run(self):
        """Main execution flow for daily ETL."""
        self.setup()

        is_automated = self.target_date is None

        feature_processor = FeatureProcessor()

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
                        path = os.path.join(
                            'data/processed',
                            exchange_id,
                            mkt_type,
                            symbol.replace('/','-'),
                            data_type,
                            f"{current_date.replace('-','')}.parquet"
                        )
                        if not os.path.exists(path):
                            self.daily_feature_consolidation(
                                symbol=symbol,
                                exchange_id=exchange_id,
                                mkt_type=mkt_type,
                                data_type=data_type,
                                current_date=current_date
                            )
                            feature_processor.process_daily_data(
                                exchange_id=exchange_id,
                                mkt_type=mkt_type,
                                symbol=symbol,
                                watch_type=data_type,
                                date_str=current_date,
                                logger=self.logger
                            )
                            gc.collect()

def consolidator(target_date: str=None):
    """EntryPoint for Task Scheduler."""
    consolidator_obj = Consolidator(target_date)
    consolidator_obj.run()

if __name__ == '__main__':
    consolidator()
                            