import polars as pl
import os
import logging
import glob
from collections import Counter

from src.research.generate_features.generate_features import generate_taker_features
from src.research.strategy.taker_trend_executor import TakerTrendExecutor
from src.research.generate_label import generate_label
from src.research.bucket_test import bucket_test
from src.research.search_threshold import search_long_threshold

def setup_logger():
    os.makedirs("logs/backtest",exist_ok=True)

    logger = logging.getLogger("backtest")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)

    handler = logging.FileHandler(
        "logs/backtest/backtest.log",
        mode="w",
        encoding="utf-8"
    )

    formatter = logging.Formatter("[%(asctime)s] %(message)s")

    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger

class BacktestTakerTrendExecutor:
    def __init__(self):
        self.data_prefix = "data/processed"
        self.exchange_id = 'binance'
        self.symbol = 'BTC/USDT'

        self.strategy = TakerTrendExecutor(
            long_obi_th=0.95,
            short_obi_th=-0.95,
            min_flow=100_000,
            take_profit_bps=8,
            stop_loss_bps=-6,
            max_hold_ms=3000,
            entry_fee_bps=2.0,
            exit_fee_bps=2.0,
            cooldown_ms=1000,
        )
        self.logger = setup_logger()


    def _get_data_lake(self,exchange_id:str,mkt_type:str,symbol:str,data_type:str,days:int=0,target_date:str=None) -> pl.LazyFrame:
        match_symbol = symbol.replace('/','-')
        target_date = target_date.replace('-','') if target_date is not None else "*"

        path = os.path.join(
            self.data_prefix,
            exchange_id,
            mkt_type,
            match_symbol,
            data_type,
            f"{target_date}.parquet"
        )
        files = sorted(glob.glob(path))[-days:]

        return pl.scan_parquet(files) if files else pl.LazyFrame()
    
    def add_quantile_rank(self,df:pl.DataFrame,cols:list[str]) -> pl.DataFrame:
        return df.with_columns([
            (pl.col(c).rank(method="average") / pl.len()).alias(f"{c}_q")
            for c in cols
        ])
    
    def get_real_threshold(self,df:pl.DataFrame,params:dict[str,float]) -> dict:
        real = {}

        for q_col,q_value in params.items():
            if not q_col.endswith("_q"): continue

            raw_col = q_col[:-2]

            real[raw_col] = df.select(pl.col(raw_col).quantile(q_value)).item()

        return real
    
    def main(self):
        day = 2

        orderbook_spot = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type='spot',
            symbol=self.symbol,
            data_type='orderbook',
            days=day
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
            days=day
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
            days=day
        ).select(['timestamp','price','amount','side']).sort('timestamp')

        trades_future = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type='future',
            symbol=self.symbol,
            data_type='trades',
            days=day
        ).select(['timestamp','price','amount','side']).sort('timestamp')

        mark_price = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type='future',
            symbol=self.symbol,
            data_type='mark_price',
            days=day
        ).select(['mark_price','timestamp']).sort('timestamp')

        open_interest = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type='future',
            symbol=self.symbol,
            data_type='open_interest',
            days=day
        ).select(['base_volume','open_interest_amount','timestamp']).sort('timestamp')

        df = generate_taker_features(
            df_orderbook_spot=orderbook_spot,
            df_orderbook_future=orderbook_future,
            df_trades_spot=trades_spot,
            df_trades_future=trades_future,
            df_mark_price=mark_price,
            df_open_interest=open_interest
        ).collect()

        df = df.drop_nulls([
            "future_trade_flow_1s",
            "future_ob_ofi_1s",
            "future_obi_l1",
            "spot_obi_l1",
            "best_bid",
            "best_ask",
        ])

        df = self.add_quantile_rank(df,[
            "future_obi_l1",
            "spot_obi_l1",
            "future_ob_ofi_1s",
            "future_ob_ofi_2s",
            "future_trade_flow_1s",
            "future_trade_flow_2s",
            "spot_trade_flow_1s",
            "spot_trade_flow_2s",
        ])

        for horizon in [1000,2000,3000,4000,5000]:
            print(f"======={horizon}========")
            df_labeled = generate_label(df,horizon_ms=horizon,tick_ms=100)

            result = search_long_threshold(df_labeled)

            for row in result.head(5).iter_rows(named=True):
                params = {
                    k: v
                    for k, v in row.items()
                    if k.endswith("_q")
                }

                real = self.get_real_threshold(df_labeled, params)

                print("-------------")
                print(row)
                print("real:", real)

        trades = self.strategy.main(df)

        for i,trade in enumerate(trades):
            self.logger.info(f"""
[{i}]
side            : {trade['side']}

entry_ts        : {trade['entry_ts']}
exit_ts         : {trade['exit_ts']}
hold_ms         : {trade['hold_ms']}

entry_price     : {trade["entry_price"]:.2f}
exit_price      : {trade["exit_price"]:.2f}

gross_bps       : {trade["gross_bps"]:.4f}
net_bps         : {trade["net_bps"]:.4f}

reason          : {trade["reason"]}

entry_signal:
future_obi_l1={trade["future_obi_l1"]:.3f}
spot_obi_l1={trade["spot_obi_l1"]:.3f}
future_trade_flow_1s={trade["future_trade_flow_1s"]:.0f}
future_ob_ofi_1s={trade["future_ob_ofi_1s"]}
spread_future={trade["spread_future"]}
            """)

        if len(trades) == 0:
            self.logger.info("[REPORT] no trades")
        else:
            avg_net = (
                sum(r["net_bps"] for r in trades)
                / len(trades)
            )

            win_rate = (
                sum(
                    1
                    for r in trades
                    if r["net_bps"] > 0
                )
                / len(trades)
            )

            avg_gross = (
                sum(r["gross_bps"] for r in trades)
                / len(trades)
            )
        self.logger.info(
f"""
===== REPORT =====

trade_count : {len(trades)}

avg_gross   : {avg_gross:.4f}
avg_net     : {avg_net:.4f}

win_rate    : {win_rate:.2%}

gross_sum   : {sum(r["gross_bps"] for r in trades):.2f}
net_sum     : {sum(r["net_bps"] for r in trades):.2f}

take_profit :
{sum(r["reason"]=="take_profit" for r in trades)}

stop_loss :
{sum(r["reason"]=="stop_loss" for r in trades)}

max_hold :
{sum(r["reason"]=="max_hold" for r in trades)}

"""
    )

if __name__ == "__main__":
    obj_model = BacktestTakerTrendExecutor()
    obj_model.main()

# future_obi_l1_q:0.95
# spot_obi_l1_q:0.9
# future_ob_ofi_1s_q:0.9
# future_ob_ofi_2s_q:0.9
# future_trade_flow_1s_q:0.9
# future_trade_flow_2s_q:0.9
# spot_trade_flow_1s_q:0.9
# spot_trade_flow_2s_q:0.9