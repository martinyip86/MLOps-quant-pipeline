from research.train_maker_as import TrainMakerAS
from research.train_taker_as import TrainTakerAS
from research.high_freq_taker_strategy_V1 import HighFreqTakerStrategy_V1
from research.high_freq_taker_strategy_V2 import HighFreqTakerStrategy_V2
from research.generate_features.generate_features import generate_maker_features,generate_features_v1

import polars as pl
import os
import glob

class TrainAlpha:
    def __init__(self):
        self.data_predix = "data/processed"
        self.exchange_id = 'binance'
        self.symbol = 'BTC/USDT'

    def _get_data_lake(self,exchange_id:str,mkt_type:str,symbol:str,data_type:str,days:int=0,target_date:str=None) -> pl.LazyFrame:
        match_symbol = symbol.replace('/','-')
        target_date = target_date.replace('-','') if target_date is not None else "*"

        path = os.path.join(
            self.data_predix,
            exchange_id,
            mkt_type,
            match_symbol,
            data_type,
            f"{target_date}.parquet"
        )
        files = sorted(glob.glob(path))[-days:]

        return pl.scan_parquet(files) if files else pl.LazyFrame()

    def main(self):
        orderbook_spot = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type='spot',
            symbol=self.symbol,
            data_type='orderbook',
            days=1
        ).select([
            'timestamp',
            pl.col('bid_prices').alias('bid_prices_spot'),
            pl.col('bid_volumes').alias('bid_volumes_spot'),
            pl.col('ask_prices').alias('ask_prices_spot'),
            pl.col('ask_volumes').alias('ask_volumes_spot'),
            pl.col('micro_price').alias('micro_price_spot'),
            pl.col('spread').alias('spread_spot'),
            pl.col('mid_price').alias('mid_price_spot'),
            pl.col('sim_buy_price_avg').alias('sim_buy_price_avg_spot'),
            pl.col('buy_impact_bps').alias('buy_impact_bps_spot')
        ]).sort('timestamp')

        orderbook_future = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type='future',
            symbol=self.symbol,
            data_type='orderbook',
            days=1
        ).select([
            'timestamp',
            pl.col('bid_prices').alias('bid_prices_future'),
            pl.col('bid_volumes').alias('bid_volumes_future'),
            pl.col('ask_prices').alias('ask_prices_future'),
            pl.col('ask_volumes').alias('ask_volumes_future'),
            pl.col('micro_price').alias('micro_price_future'),
            pl.col('spread').alias('spread_future'),
            pl.col('mid_price').alias('mid_price_future'),
            pl.col('sim_buy_price_avg').alias('sim_buy_price_avg_future'),
            pl.col('buy_impact_bps').alias('buy_impact_bps_future')
        ]).sort('timestamp')

        trades_spot = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type='spot',
            symbol=self.symbol,
            data_type='trades',
            days=1
        ).select(['timestamp','price','amount','side','is_taker_buyer']).sort('timestamp')

        trades_future = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type='future',
            symbol=self.symbol,
            data_type='trades',
            days=1
        ).select(['timestamp','price','amount','side']).sort('timestamp')

        mark_price = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type='future',
            symbol=self.symbol,
            data_type='mark_price',
            days=1
        ).select(['mark_price','timestamp']).sort('timestamp')

        open_interest = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type='future',
            symbol=self.symbol,
            data_type='open_interest',
            days=1
        ).select(['base_volume','open_interest_amount','timestamp']).sort('timestamp')

        orderbook = generate_features_v1(
            df_orderbook_spot=orderbook_spot,
            df_orderbook_future=orderbook_future,
            df_trades_spot=trades_spot,
            df_trades_future=trades_future,
            df_mark_price=mark_price,
            df_open_interest=open_interest
        )

        # print(orderbook.collect().head(20))
        # maker_model = TrainTakerAS()
        # pnl = maker_model.run_taker_backtest(df_orderbook=orderbook.collect())

        taker_model = HighFreqTakerStrategy_V2()
        pnl = taker_model.main(df=orderbook.collect())
        

if __name__ == '__main__':
    obj = TrainAlpha()
    obj.main()