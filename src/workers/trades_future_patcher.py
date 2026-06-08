from src.workers.base_patcher import BasePatcher
import polars as pl
import os
import time

class TradesFuturetPatcher(BasePatcher):
    def __init__(self,exchange_id:str,symbol:str,target_date:str,logger):
        super().__init__(exchange_id,symbol,target_date,logger)
        self.mkt_type = 'future'

    def _get_url(self,exchange_id:str,symbol:str,target_date:str):
        binance_symbol = symbol.replace('/','').replace('-','')
        okx_symbol = symbol.replace('/','-')
        clear_date = target_date.replace('-','')
        urls = {
            'binance':{
                'url':f"https://data.binance.vision/data/futures/um/daily/trades/{binance_symbol}/{binance_symbol}-trades-{target_date}.zip",
                'file_path':f"temp/{exchange_id}/future/{binance_symbol}-trades-{target_date}.csv"
            },
            'okx':{
                'url':f"https://static.okx.com/cdn/okex/traderecords/trades/daily/{clear_date}/{okx_symbol}-SWAP-trades-{target_date}.zip",
                'file_path':f"temp/{exchange_id}/future/{okx_symbol}-SWAP-trades-{target_date}.csv"
            }
        }
        exchange_data = urls[exchange_id]
        url = exchange_data['url']
        file_path = exchange_data['file_path']
        return url,file_path
    
    def _clear_data(self,exchange_id:str,mkt_type:str,symbol:str,file_path:str) -> pl.LazyFrame:
        columns = {
            'binance':{
                'header':["trade_id","price","amount","cost","timestamp","is_maker"],
                'clear_columns':[
                    pl.lit(exchange_id).alias('exchange_id'),
                    pl.lit(symbol).alias('symbol'),
                    pl.lit(mkt_type).alias('mkt_type'),
                    pl.col('id').cast(pl.Int64).alias('trade_id'),
                    pl.col('price').cast(pl.Float64),
                    pl.col('qty').cast(pl.Float64).alias('amount'),
                    pl.when(pl.col('time').cast(pl.String).str.len_chars() == 16).then(pl.col('time').cast(pl.Int64) // 1000).otherwise(pl.col('time').cast(pl.Int64)).alias('timestamp'),
                    pl.when(pl.col('is_buyer_maker') == False).then(pl.lit('buy')).otherwise(pl.lit('sell')).alias('side'),
                    pl.lit(int(time.time() * 1000)).alias('local_timestamp')
                ],
                'select':['trade_id','exchange_id','symbol','mkt_type','price','amount','timestamp','side','local_timestamp']
            },
            'okx':{
                'header':["symbol","trade_id","side","price","amount","timestamp"],
                'clear_columns':[
                    pl.lit(exchange_id).alias('exchange_id'),
                    pl.lit(symbol).alias('symbol'),
                    pl.lit(mkt_type).alias('mkt_type'),
                    pl.col('trade_id').cast(pl.Int64),
                    pl.col('price').cast(pl.Float64),
                    pl.col('size').cast(pl.Float64).alias('amount'),
                    pl.col("created_time").cast(pl.Int64).alias('timestamp'),
                    pl.lit(int(time.time() * 1000)).alias('local_timestamp')
                ],
                'select':['trade_id','exchange_id','symbol','mkt_type','price','amount','timestamp','side','local_timestamp']
            }
        }
        target_col = columns[exchange_id]
        clear_columns = target_col['clear_columns']
        schema = target_col['select']
        return pl.scan_csv(file_path).with_columns(clear_columns).select(schema).filter((pl.col('price') > 0) & (pl.col('amount') > 0))
    
    def _get_ch_data(self,exchange_id:str,symbol:str,max_trade_id,min_trade_id) -> pl.LazyFrame:
        sql = f"""
            SELECT trade_id FROM market_data.trades_future
            WHERE trade_id BETWEEN {min_trade_id} AND {max_trade_id}
                AND exchange_id='{exchange_id}'
                AND symbol='{symbol}'
            ORDER BY trade_id ASC
        """
        arrow = self.ch.query_arrow(sql)
        if arrow.num_rows == 0:
            return pl.LazyFrame()
        else:
            return pl.from_arrow(arrow).lazy()
        
    def _verify_full_integrity(self,exchange_id:str,symbol:str,official_df:pl.DataFrame,file_path:str,max_trade_id,min_trade_id):
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
                    side
                FROM
                    trades_future
                WHERE exchange_id='{exchange_id}' AND symbol='{symbol}'
                    AND trade_id BETWEEN {min_trade_id} AND {max_trade_id}
                ORDER BY trade_id ASC
            """
            settings = {
                'max_threads': 1,               # 必须为1，严禁并发
                'max_block_size': 500,         # 极其重要：从 8192 降到 1000，减小服务器单次读取的负担
                'max_memory_usage': '1G',       # 限制服务器使用的总内存
                'preferred_block_size_bytes': '1048576',
            }
            column_names = ['trade_id','price','amount','timestamp','side']
            chunks = []
            with self.ch.query_column_block_stream(sql,settings=settings) as stream:
                for block in stream:
                    if not block: continue
                    chunk_df = pl.from_dict(dict(zip(column_names,block)))
                    chunks.append(chunk_df)

            if chunks:
                ch_df:pl.DataFrame = pl.concat(chunks,rechunk=True)
                del chunks

                diff = csv_df.join(ch_df,on='trade_id',how='anti')

                if diff.is_empty() and len(csv_df) == len(ch_df):
                    self.logger.info(f"💎 [AUDIT-PASSED] 100% Data Integrity for {exchange_id} {symbol}.")
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    return True
                else:
                    self.logger.error(f"🚨 [AUDIT-FAILED] Mismatch detected! Gaps found: {len(diff)}")
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    return False

        except Exception as e:
            self.logger.error(f"🚨 [AUDIT-CRASH] Audit process failed: {e}")
            return False
    
    def main(self):
        url,file_path = self._get_url(self.exchange_id,self.symbol,self.target_date)
        exists_ok = self._download_csv(self.exchange_id,self.mkt_type,url,file_path)
        if exists_ok and os.path.exists(file_path):
            official_lf = self._clear_data(self.exchange_id,self.mkt_type,self.symbol,file_path)
            stats = official_lf.select([
                pl.col('trade_id').max().alias('max_id'),
                pl.col('trade_id').min().alias('min_id')
            ]).collect(streaming=True)
            max_trade_id = stats['max_id'][0]
            min_trade_id = stats['min_id'][0]
            ch_lf = self._get_ch_data(self.exchange_id,self.symbol,max_trade_id,min_trade_id)
            gaps_df = pl.DataFrame()
            if not ch_lf.collect().is_empty():
                gap_lf = official_lf.join(ch_lf,on='trade_id',how='anti')
                if not gap_lf.collect().is_empty():
                    gaps_df = gap_lf.collect()
            else:
                gaps_df = official_lf.collect()

            if not gaps_df.is_empty():
                try:
                    self.sync_to_clickhouse(gaps_df,'trades_future')
                    self.logger.info(f"✅ [PATCHED] Injected {len(gaps_df)} missing records into {self.exchange_id} {self.symbol}.")
                    partition_id = self.target_date.replace('-','')
                    sql = f"OPTIMIZE TABLE market_data.trades_future PARTITION {partition_id} FINAL"
                    self.ch.command(sql)
                    time.sleep(1)
                except Exception as e:
                    self.logger.error(f"❌ [GAP-ERROR] Patch failed: {e}")

            self._verify_full_integrity(
                exchange_id=self.exchange_id,
                symbol=self.symbol,
                official_df=official_lf.collect(),
                file_path=file_path,
                max_trade_id=max_trade_id,
                min_trade_id=min_trade_id
            )