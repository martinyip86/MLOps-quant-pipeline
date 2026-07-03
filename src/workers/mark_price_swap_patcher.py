from src.workers.base_patcher import BasePatcher
import polars as pl
import os
import time


class MarkPriceSwapPatcher(BasePatcher):
    def __init__(self,exchange_id:str,symbol:str,target_date:str,logger):
        super().__init__(exchange_id,symbol,target_date,logger)
        self.mkt_type = 'swap'

    def _get_url(self,exchange_id:str,symbol:str,target_date:str):
        binance_symbol = symbol.replace('/','').replace('-','')
        okx_symbol = symbol.replace('/','-')
        clear_date = target_date.replace('-','')
        urls = {
            'binance':{
                'url':f"https://data.binance.vision/data/futures/um/daily/markPriceKlines/{binance_symbol}/1m/{binance_symbol}-1m-{target_date}.zip",
                'file_path':f"temp/{exchange_id}/swap/{binance_symbol}-1m-{target_date}.csv"
            }
        }
        if exchange_id in urls:
            exchange_data = urls[exchange_id]
            url = exchange_data['url']
            file_path = exchange_data['file_path']
            return url,file_path
        else:
            return False,False
    
    def _clear_data(self,exchange_id:str,mkt_type:str,symbol:str,file_path:str) -> pl.LazyFrame:
        columns = {
            'binance':{
                'header':["open_time","open","high","low","close","volume","close_time","quote_volume","count","taker_buy_volume","taker_buy_quote_volume","ignore"],
                'clear_columns':[
                    pl.lit(exchange_id).alias('exchange_id'),
                    pl.lit(symbol).alias('symbol'),
                    pl.lit(mkt_type).alias('mkt_type'),
                    pl.col('close').cast(pl.Float64).alias('mark_price'),
                    pl.col('close_time').cast(pl.Int64).alias('timestamp'),
                    pl.lit(0.0).alias('index_price'),
                    pl.lit(int(time.time() * 1000)).alias('local_timestamp')
                ],
                'select':['exchange_id','symbol','mkt_type','mark_price','index_price','timestamp','local_timestamp']
            }
        }
        target_col = columns[exchange_id]
        header = target_col['header']
        clear_columns = target_col['clear_columns']
        schema = target_col['select']
        return pl.scan_csv(file_path).with_columns(clear_columns).select(schema)
    
    def _get_ch_data(self,exchange_id:str,symbol:str,max_timestamp,min_timestamp) -> pl.LazyFrame:
        sql = f"""
            SELECT timestamp FROM market_data.market_price_swap
            WHERE timestamp BETWEEN {min_timestamp} AND {max_timestamp}
                AND exchange_id='{exchange_id}'
                AND symbol='{symbol}'
            ORDER BY timestamp ASC
        """
        arrow = self.ch.query_arrow(sql)
        if arrow.num_rows == 0:
            return pl.LazyFrame()
        else:
            return pl.from_arrow(arrow).lazy()
    
    def main(self):
        url,file_path = self._get_url(self.exchange_id,self.symbol,self.target_date)
        if url and file_path:
            exists_ok = self._download_csv(self.exchange_id,self.mkt_type,url,file_path)
            if exists_ok and os.path.exists(file_path):
                official_lf = self._clear_data(self.exchange_id,self.mkt_type,self.symbol,file_path)
                stats = official_lf.select([
                    pl.col('timestamp').max().alias('max_timestamp'),
                    pl.col('timestamp').min().alias('min_timestamp')
                ]).collect(streaming=True)
                max_timestamp = stats['max_timestamp'][0]
                min_timestamp = stats['min_timestamp'][0]
                ch_lf = self._get_ch_data(self.exchange_id,self.symbol,max_timestamp,min_timestamp)
                gaps_df = pl.DataFrame()
                if not ch_lf.collect().is_empty():
                    gap_lf = official_lf.join(ch_lf,on='timestamp',how='anti')
                    if not gap_lf.collect().is_empty():
                        gaps_df = gap_lf.collect()
                else:
                    gaps_df = official_lf.collect()

                if not gaps_df.is_empty():
                    try:
                        # self.sync_to_clickhouse(gaps_df,'market_price_swap')
                        self.export_parquet(gaps_df,'market_price_swap')
                        self.logger.info(f"✅ [PATCHED] Injected {len(gaps_df)} missing records into {self.exchange_id} {self.symbol}.")
                        # partition_id = self.target_date.replace('-','')
                        # sql = f"OPTIMIZE TABLE market_data.market_price_swap PARTITION {partition_id} FINAL"
                        # self.ch.command(sql)
                        # time.sleep(1)
                    except Exception as e:
                        self.logger.error(f"❌ [GAP-ERROR] Patch failed: {e}")
                    finally:
                        if os.path.exists(file_path):
                            os.remove(file_path)