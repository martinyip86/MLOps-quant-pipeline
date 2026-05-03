from src.storage.clickhouse.client import ch_manager
from datetime import datetime,timezone
import polars as pl
import glob
import os

class LocalData:
    def __init__(self):
        self.ch = ch_manager.connect()

    def get_orderbook_ch_data(self,exchange_id:str="binance",symobl:str="BTC/USDT",mkt_type:str="spot",date:str=None) -> pl.LazyFrame:
        table_name = f"orderbook_{mkt_type}"
        if date is None:
            date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        sql = f"""
            SELECT
                timestamp,
                bid_prices,
                bid_volumes,
                ask_prices,
                ask_volumes,
                ((bid_prices[1] * ask_volumes[1] + ask_prices[1] * bid_volumes[1]) / nullif(bid_volumes[1] + ask_volumes[1],0)) AS micro_price,
                (ask_prices[1] - bid_prices[1]) AS spread,
                ((bid_prices[1] + ask_prices[1]) / 2) AS mid_price,
                (
                    arraySum(
                        arrayMap(
                            (p,v) -> p * v,
                            arraySlice(ask_prices,1,20),
                            arraySlice(ask_volumes,1,20)
                        )
                    ) /
                    nullif(arraySum(arraySlice(ask_volumes,1,20)))
                ) AS sim_buy_avg,
                ((sim_buy_avg / mid_price) - 1) * 10000 AS buy_impact_bps
            FROM {table_name}
            WHERE exchange_id='{exchange_id}'
                AND mkt_type='{mkt_type}'
                AND symbol='{symobl}'
                AND toDate(fromUnixTimestamp64Milli(timestamp))='{date}'
            ORDER BY timestamp ASC
        """
        arrow = self.ch.query_arrow(sql)
        return pl.from_arrow(arrow).lazy()
    
    def get_trades_ch_data(self,exchange_id:str="binance",symobl:str="BTC/USDT",mkt_type:str="spot",date:str=None) -> pl.LazyFrame:
        table_name = f"trades_{mkt_type}"
        if date is None:
            date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        sql = f"""
            SELECT
                price,
                amount,
                side,
                timestamp
            FROM {table_name}
            WHERE exchange_id='{exchange_id}'
                AND mkt_type='{mkt_type}'
                AND symbol='{symobl}'
                AND toDate(fromUnixTimestamp64Milli(timestamp))='{date}'
            ORDER BY timestamp ASC
        """
        arrow = self.ch.query_arrow(sql)
        return pl.from_arrow(arrow).lazy()
    
    def get_orderbook_cool_data(self,exchange_id:str="binance",symobl:str="BTC/USDT",mkt_type:str="spot",days:int=5) -> pl.LazyFrame:
        path = os.path.join(
            "data/processed",
            exchange_id,
            mkt_type,
            symobl.replace('/','-'),
            f"orderbook/*.parquet"
        )
        files = sorted(glob.glob(path))[-days:]

        if not files:
            return pl.LazyFrame()
        
        return pl.scan_parquet(files).select(['timestamp','bid_prices','bid_volumes','ask_prices','ask_volumes','micro_price','imbalance','spread','mid_price','sim_buy_price_avg','buy_impact_bps'])
    
    def get_trades_cool_data(self,exchange_id:str="binance",symobl:str="BTC/USDT",mkt_type:str="spot",days:int=5) -> pl.LazyFrame:
        path = os.path.join(
            "data/processed",
            exchange_id,
            mkt_type,
            symobl.replace('/','-'),
            f"trades/*.parquet"
        )
        files = sorted(glob.glob(path))[-days:]

        if not files:
            return pl.LazyFrame()

        return pl.scan_parquet(files).select(['price','amount','side','timestamp'])
    
    def get_kline_data(self,exchange_id:str="binance",symobl:str="BTC/USDT",mkt_type:str="spot",interval:str="5m",days:int=5) -> pl.LazyFrame:
        path = os.path.join(
            "data/kline",
            exchange_id,
            mkt_type,
            symobl.replace('/','-'),
            interval,
            "*.parquet"
        )
        files = sorted(glob.glob(path))[-days:]
        if not files:
            return pl.LazyFrame()

        return pl.scan_parquet(files)