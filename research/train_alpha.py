from src.storage.clickhouse.client import ch_manager
from src.utils.weight_manager import WeightManager
from src.analytics.indicators import calc_rsi_expr,calc_macd_expr,calc_ema50_expr,calc_volume_ma_expr,calc_atr_expr
from src.analytics.local_data import LocalData
from research.factor_analysis import AlphaResearch
from research.backtest.market_making_backtest import MarketMakingBacktest
from datetime import datetime,timezone,timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import polars as pl
import numpy as np
import glob
import os
import json
import sys

sns.set_theme(style='darkgrid')

class TrainAlpha:
    def __init__(self,exchange_id:str='binance',mkt_type:str='spot',symbol:str='BTC/USDT'):
        self.ch = None
        self.exchange_id = exchange_id
        self.mkt_type = mkt_type
        self.symbol = symbol
    
    def add_htf_trend(self,main_df:pl.LazyFrame,htf_df:pl.LazyFrame) -> pl.LazyFrame:
        htf_df = htf_df.with_columns([
            pl.col('close').ewm_mean(span=50,adjust=False).alias('htf_ema50')
        ]).with_columns([
            (pl.col('close') / pl.col('htf_ema50') - 1).alias('htf_trend_ratio')
        ]).select(['timestamp','htf_trend_ratio'])

        return main_df.join_asof(htf_df.sort('timestamp'),on='timestamp',strategy='backward')
    
    def add_dist_to_support(self,df:pl.LazyFrame,window:int=20) -> pl.LazyFrame:
        return df.with_columns([
            pl.col('low').rolling_min(window_size=window).alias('support_level')
        ]).with_columns([
            (pl.col('close') / pl.col('support_level') - 1).alias('dist_to_support')
        ])
    
    def add_liquidity_sweep(self,df:pl.LazyFrame,window:int=30) -> pl.LazyFrame:
        return df.with_columns([
            pl.col('low').shift(1).rolling_min(window_size=window).alias('prev_low')
        ]).with_columns([
            pl.when((pl.col('low') < pl.col('prev_low')) & (pl.col('close') > pl.col('prev_low'))).then(1).otherwise(0).alias('is_sweep')
        ])
    
    def _save_weight(self,weights):
        model_weight = WeightManager()
        old_weight = model_weight.load_weight(self.exchange_id,self.mkt_type,self.symbol,'orderbook')

        if old_weight is not None:
            alpha = 0.2
            old_coef = np.array(old_weight['coef'])
            new_coef = np.array(weights['coef'])
            weights['coef'] = (
                old_coef * (1 - alpha) + new_coef * alpha
            ).tolist()

            weights['intercept'] = old_weight['intercept'] * (1 - alpha) + weights['intercept'] * alpha
            weights['signal_scale'] = old_weight['signal_scale'] * (1 - alpha) + weights['signal_scale'] * alpha

        path = model_weight.save_weight(weights,self.exchange_id,self.mkt_type,self.symbol,'orderbook')
        print(f"Traning completed,model save to {path}")

    def main(self):
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        self.ch = ch_manager.connect()
        local_data = LocalData()

        ob_df = local_data.get_orderbook_cool_data(
            exchange_id=self.exchange_id,
            symobl=self.symbol,
            mkt_type=self.mkt_type,
            days=5
        ).sort('timestamp')

        td_df = local_data.get_trades_cool_data(
            exchange_id=self.exchange_id,
            symobl=self.symbol,
            mkt_type=self.mkt_type,
            days=5
        ).sort('timestamp')

        td_df = td_df.with_columns([
            pl.from_epoch('timestamp',time_unit="ms")
        ]).sort('timestamp').rolling(index_column='timestamp',period="1s",closed="left").agg([
            pl.col('amount').filter(pl.col('side') == 'buy').sum().fill_null(0).alias('buy_vol_1s'),
            pl.col('amount').filter(pl.col('side') == 'sell').sum().fill_null(0).alias('sell_vol_1s'),
            (pl.col('amount') * pl.when(pl.col('side') == 'buy').then(1).otherwise(-1)).sum().alias('net_volume_1s'),
            pl.len().alias('trade_count_1s'),
            ((pl.col('price').mean() / pl.col('price').last()) - 1).alias('price_drift_1s')
        ])

        df = ob_df.with_columns([
            pl.from_epoch('timestamp',time_unit="ms")
        ]).join_asof(td_df,on='timestamp',strategy="backward")

        kl_df = local_data.get_kline_data(
            exchange_id=self.exchange_id,
            symobl=self.symbol,
            mkt_type=self.mkt_type,
            interval="1m",
            days=5
        )

        kl_df = kl_df.pipe(self.add_dist_to_support).pipe(self.add_liquidity_sweep)

        htf_df = local_data.get_kline_data(
            exchange_id=self.exchange_id,
            symobl=self.symbol,
            mkt_type=self.mkt_type,
            interval="1h",
            days=5
        ).with_columns([
            pl.from_epoch('open_time',time_unit='ms').alias('timestamp')
        ]).sort('timestamp')

        kl_df = kl_df.with_columns([
            pl.from_epoch('open_time',time_unit="ms"),
            calc_rsi_expr(),
            calc_macd_expr(),
            calc_ema50_expr(),
            calc_volume_ma_expr(),
            calc_atr_expr()
        ])

        df = df.join_asof(kl_df.rename({'open_time':'timestamp'}),on='timestamp',strategy="backward")
        df = self.add_htf_trend(main_df=df,htf_df=htf_df).collect()

        df = df.drop_nulls()

        last_date = df.select(pl.col('timestamp').dt.date().max()).item()
        train_df = df.filter(pl.col('timestamp').dt.date() < last_date)

        research = AlphaResearch(train_df)
        research.compute_features().label_data().select_best_lag()

        # self._save_weight(research.weights)

if __name__=='__main__':
    trainAlpha = TrainAlpha()
    trainAlpha.main()