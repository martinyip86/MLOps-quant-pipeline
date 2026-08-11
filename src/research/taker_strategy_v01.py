import polars as pl
import numpy as np
import os
import glob
import logging

from src.research.generate_features.generate_features import generate_taker_features
from src.research.generate_label import generate_label
from src.research.config.risk_config import RiskConfig
from src.research.training.train_model import TrainModel

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

class TakerStrategy:
    def __init__(self):
        self.logger = setup_logger()
        self.risk = RiskConfig()
        self.training_model = TrainModel()

        self.data_prefix = "data/processed"
        self.exchange_id = "binance"
        self.symbol = "BTC/USDT"

        # self.balance = 50_000
        # self.stop_loss_bps = 6.0
        # self.take_profit_bps = 8.0
        # self.fee_bps = 2.0
        # self.max_hold_ms = 60_000 * 15

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

    def main(self):
        day = 7
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

        orderbook_swap = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type="swap",
            symbol=self.symbol,
            data_type='orderbook',
            days=day
        ).select([
            'timestamp',
            pl.col('bid_prices').alias('bid_prices_swap'),
            pl.col('bid_volumes').alias('bid_volumes_swap'),
            pl.col('ask_prices').alias('ask_prices_swap'),
            pl.col('ask_volumes').alias('ask_volumes_swap'),
            pl.col('micro_price').alias('micro_price_swap'),
            pl.col('spread').alias('spread_swap'),
            pl.col('mid_price').alias('mid_price_swap'),
            pl.col('sim_buy_price_avg').alias('sim_buy_price_avg_swap'),
            pl.col('buy_impact_bps').alias('buy_impact_bps_swap')
        ]).sort('timestamp')

        trades_spot = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type='spot',
            symbol=self.symbol,
            data_type='trades',
            days=day
        ).select(['timestamp','price','amount','side']).sort('timestamp')

        trades_swap = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type="swap",
            symbol=self.symbol,
            data_type='trades',
            days=day
        ).select(['timestamp','price','amount','side']).sort('timestamp')

        mark_price = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type="swap",
            symbol=self.symbol,
            data_type='mark_price',
            days=day
        ).select(['mark_price','timestamp']).sort('timestamp')

        open_interest = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type="swap",
            symbol=self.symbol,
            data_type='open_interest',
            days=day
        ).select(['base_volume','open_interest_amount','timestamp']).sort('timestamp')

        df = generate_taker_features(
            df_orderbook_spot=orderbook_spot,
            df_orderbook_swap=orderbook_swap,
            df_trades_spot=trades_spot,
            df_trades_swap=trades_swap,
            df_mark_price=mark_price,
            df_open_interest=open_interest
        ).collect()

        df = df.drop_nulls()

        rows = df.rows(named=True)

        training_records = []

        sample_interval = 60_000
        last_sample_timestamp = None

        for current_index,current_row in enumerate(rows):
            entry_timestamp = current_row["timestamp"]
            entry_price = current_row["best_ask"]

            if last_sample_timestamp is not None and entry_timestamp - last_sample_timestamp < sample_interval:
                continue

            last_sample_timestamp = entry_timestamp

            if rows[-1]["timestamp"] - entry_timestamp < self.risk.max_hold_ms:
                break

            max_net_bps = float("-inf")
            min_net_bps = float("inf")

            max_index = None
            min_index = None
            end_index = None
            end_net_bps = None

            label = "neutral"

            volatility_horizon_ms = 5 * 60_000
            volatility_threshold_bps = 12.0

            current_price = current_row["mid_price_swap"]

            past_max_price = current_price
            past_min_price = current_price

            for past_index in range(current_index-1,-1,-1):
                past_row = rows[past_index]

                past_used_time_ms = entry_timestamp - past_row["timestamp"]

                if past_used_time_ms > volatility_horizon_ms: break

                past_price = past_row["mid_price_swap"]

                past_max_price = max(past_max_price,past_price)
                past_min_price = min(past_min_price,past_price)

            past_range_bps_5m = (past_max_price - past_min_price) / current_price * 10_000

            future_max_price = current_price
            future_min_price = current_price

            for future_index in range(current_index+1,len(rows)):
                candidate_row = rows[future_index]

                used_time_ms = candidate_row["timestamp"] - current_row["timestamp"]

                if used_time_ms <= volatility_horizon_ms:
                    future_price = candidate_row["mid_price_swap"]

                    future_max_price = max(future_max_price,future_price)
                    future_min_price = min(future_min_price,future_price)

                if used_time_ms > self.risk.max_hold_ms:
                    result = "time_out"
                    break

                exit_index = future_index
                exit_price = candidate_row["best_bid"]

                net_bps = (
                    (exit_price / entry_price - 1) * 10_000 - self.risk.fee_bps_per_side * 2
                )

                end_index = future_index
                end_net_bps = net_bps

                if net_bps > max_net_bps:
                    max_net_bps = net_bps
                    max_index = future_index

                if net_bps < min_net_bps:
                    min_net_bps = net_bps
                    min_index = future_index

            if end_index is None: continue

            future_up_bps = (future_max_price / current_price - 1) * 10_000
            future_down_bps = (current_price / future_min_price - 1) * 10_000

            future_move_bps = max(future_up_bps,future_down_bps)

            volatility_trigger = int(future_move_bps >= volatility_threshold_bps)

            if max_net_bps >= 8 and min_net_bps > -16:
                label = "safe_profit"
            elif max_net_bps < 0:
                label = "no_profit"

            time_to_max_minutes = (rows[max_index]["timestamp"] - entry_timestamp) / 60_000

            time_to_min_minutes = (rows[min_index]["timestamp"] - entry_timestamp) / 60_000

            record = {
                **current_row,

                "entry_row":current_index,
                "entry_timestamp":entry_timestamp,
                
                "max_net_bps":max_net_bps,
                "time_to_max_minutes":time_to_max_minutes,
                "max_row":max_index,

                "min_net_bps":min_net_bps,
                "time_to_min_minutes":time_to_min_minutes,
                "min_row":min_index,

                "end_net_bps":end_net_bps,
                "end_row":end_index,
                "end_timestamp":rows[end_index]["timestamp"],

                "past_range_bps_5m": past_range_bps_5m,
                "future_move_bps_5m": future_move_bps,
                "future_up_bps_5m":future_up_bps,
                "future_down_bps_5m":future_down_bps,
                "volatility_trigger":volatility_trigger,

                "label":label
            }

            self.logger.info(record)
            training_records.append(record)

        train_df = pl.DataFrame(training_records)

        print(train_df.select([
            pl.len().alias("total"),
            pl.col("volatility_trigger").sum().alias("trigger_count"),
            pl.col("volatility_trigger").mean().alias("trigger_ratio")
        ]))

        self.training_model.training(train_df)
                
                

if __name__ == "__main__":
    obj = TakerStrategy()
    obj.main()