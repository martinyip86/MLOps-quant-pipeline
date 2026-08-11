import polars as pl
import numpy as np
import os

from datetime import datetime,timezone,timedelta

class Test:
    def __init__(self):
        pass

    def main(self):
        today = datetime.now(timezone.utc) - timedelta(days=1)
        today = today.strftime("%Y%m%d")
        path = os.path.join(
            "data/processed/binance/swap/BTC-USDT/orderbook",
            f"{today}.parquet"
        )

        df = pl.read_parquet(path)

        info = df.with_columns([
            pl.col("bid_prices").list.get(0).alias("bid_price"),
            pl.col("timestamp").cast(pl.Datetime("ms"))
        ]).with_columns([
            # pl.col("bid_price").rolling_var(window_size=100,ddof=0).alias("variance_100"),
            # pl.col("bid_price").rolling_std(window_size=100,ddof=0).alias("standard_100"),
            # pl.col("bid_price").rolling_mean(window_size=100).alias("mean_100"),
            pl.col("bid_price").rolling_var_by(by="timestamp",window_size="1h",ddof=0).alias("variance_1h"),
            pl.col("bid_price").rolling_std_by(by="timestamp",window_size="1h",ddof=0).alias("standard_1h"),
            pl.col("bid_price").rolling_mean_by(by="timestamp",window_size="1h").alias("mean_1h")
        ]).with_columns([
            (pl.col("standard_1h") / pl.col("mean_1h") * 100).alias("cv_1h")
        ]).with_columns([
            ((pl.col("cv_1h") - pl.col("cv_1h").rolling_mean(100)) / pl.col("cv_1h").rolling_std(100,ddof=0)).alias("cv_zcore")
        ])

        start_time = info["timestamp"].min() + pl.duration(hours=1)

        info = info.filter(pl.col("timestamp") >= start_time).drop_nulls()

        data = info.select([
            "variance_1h","standard_1h","timestamp","cv_1h"
        ])

        print(data)

        result = data.select([
            pl.col("cv_1h").quantile(0.10).alias("low_vol_threshold"),
            pl.col("cv_1h").quantile(0.90).alias("high_vol_threshold")
        ])

        print(result)

        zcore = info.select([pl.col("cv_zcore").quantile(0.90).alias("z_high_threshold")])

        print(zcore)


if __name__ == "__main__":
    obj = Test()
    obj.main()
