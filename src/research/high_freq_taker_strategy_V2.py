import polars as pl
import numpy as np
import sys

from src.utils.logger import setup_logger

class HighFreqTakerStrategy_V2:
    def __init__(self):
        self.logger = setup_logger(
            name='high_freq_taker_strategy',
            log_file='logs/backtest/high_freq_taker_strategy.log',
            fmt='%(message)s',
            clear_on_start=True
        )
        self.friction_cost_bps = 11.0

    def main(self,df:pl.DataFrame):

        # test = df.filter(pl.col('future_return') > 0.0005)

        test_future_ofi = df.with_columns([
            pl.col('future_ofi_1s').qcut(10).alias('ofi_bucket')
        ])

        test_future_ofi = test_future_ofi.group_by('ofi_bucket').agg([
            (pl.col('future_return').mean() * 10000).alias('avg_return'),
            pl.len().alias('count')
        ])

        for row in test_future_ofi.iter_rows(named=True):
            self.logger.info(f"f_ofi_bucket: {row['ofi_bucket']} | avg_return: {row['avg_return']:.5f} bps | count: {row['count']}")

        self.logger.info(f"----------------------------")

        test_spot_ofi = df.with_columns([
            pl.col('spot_ofi_1s').qcut(10,allow_duplicates=True).alias('ofi_bucket')
        ])

        test_spot_ofi = test_spot_ofi.group_by('ofi_bucket').agg([
            (pl.col('future_return').mean() * 10000).alias('avg_return'),
            pl.len().alias('count')
        ])

        for row in test_spot_ofi.iter_rows(named=True):
            self.logger.info(f"s_ofi_bucket: {row['ofi_bucket']} | avg_return: {row['avg_return']:.5f} bps | count: {row['count']}")

        self.logger.info(f"----------------------------")

        test_future_obil1 = df.with_columns([
            pl.col('future_obi_l1').qcut(10,allow_duplicates=True).alias('obi_bucket')
        ])

        test_future_obil1 = test_future_obil1.group_by('obi_bucket').agg([
            (pl.col('future_return').mean() * 10000).alias('avg_return'),
            pl.len().alias('count')
        ])

        for row in test_future_obil1.iter_rows(named=True):
            self.logger.info(f"f_obi_l1_bucket: {row['obi_bucket']} | avg_return: {row['avg_return']:.5f} bps | count: {row['count']}")

        self.logger.info(f"----------------------------")

        test_future_obil3 = df.with_columns([
            pl.col('future_obi_l3').qcut(10,allow_duplicates=True).alias('obi_bucket')
        ])

        test_future_obil3 = test_future_obil3.group_by('obi_bucket').agg([
            (pl.col('future_return').mean() * 10000).alias('avg_return'),
            pl.len().alias('count')
        ])

        for row in test_future_obil3.iter_rows(named=True):
            self.logger.info(f"f_obi_l3_bucket: {row['obi_bucket']} | avg_return: {row['avg_return']:.5f} bps | count: {row['count']}")

        self.logger.info(f"----------------------------")

        test_future_obil5 = df.with_columns([
            pl.col('future_obi_l5').qcut(10,allow_duplicates=True).alias('obi_bucket')
        ])

        test_future_obil5 = test_future_obil5.group_by('obi_bucket').agg([
            (pl.col('future_return').mean() * 10000).alias('avg_return'),
            pl.len().alias('count')
        ])

        for row in test_future_obil5.iter_rows(named=True):
            self.logger.info(f"f_obi_l5_bucket: {row['obi_bucket']} | avg_return: {row['avg_return']:.5f} bps | count: {row['count']}")

        self.logger.info(f"----------------------------")

        test_spot_obil1 = df.with_columns([
            pl.col('spot_obi_l1').qcut(10,allow_duplicates=True).alias('obi_bucket')
        ])

        test_spot_obil1 = test_spot_obil1.group_by('obi_bucket').agg([
            (pl.col('future_return').mean() * 10000).alias('avg_return'),
            pl.len().alias('count')
        ])

        for row in test_spot_obil1.iter_rows(named=True):
            self.logger.info(f"s_obi_l1_bucket: {row['obi_bucket']} | avg_return: {row['avg_return']:.5f} bps | count: {row['count']}")

        self.logger.info(f"----------------------------")

        test_spot_obil3 = df.with_columns([
            pl.col('spot_obi_l3').qcut(10,allow_duplicates=True).alias('obi_bucket')
        ])

        test_spot_obil3 = test_spot_obil3.group_by('obi_bucket').agg([
            (pl.col('future_return').mean() * 10000).alias('avg_return'),
            pl.len().alias('count')
        ])

        for row in test_spot_obil3.iter_rows(named=True):
            self.logger.info(f"s_obi_l3_bucket: {row['obi_bucket']} | avg_return: {row['avg_return']:.5f} bps | count: {row['count']}")

        self.logger.info(f"----------------------------")

        test_spot_obil5 = df.with_columns([
            pl.col('spot_obi_l5').qcut(10,allow_duplicates=True).alias('obi_bucket')
        ])

        test_spot_obil5 = test_spot_obil5.group_by('obi_bucket').agg([
            (pl.col('future_return').mean() * 10000).alias('avg_return'),
            pl.len().alias('count')
        ])

        for row in test_spot_obil5.iter_rows(named=True):
            if row['obi_bucket'] is not None:
                self.logger.info(f"s_obi_l5_bucket: {row['obi_bucket']} | avg_return: {row['avg_return']:.5f} bps | count: {row['count']}")

        self.logger.info(f"----------------------------")

        test_f_ofi_obil1 = df.with_columns([
            pl.col('future_ofi_1s').qcut(5).alias('future_ofi_bucket'),
            pl.col('future_obi_l1').qcut(5).alias('future_obi_bucket'),
            pl.col('spot_ofi_1s').qcut(5).alias('spot_ofi_bucket'),
            pl.col('spot_obi_l1').qcut(5).alias('spot_obi_bucket'),
        ])

        test_f_ofi_obil1 = test_f_ofi_obil1.group_by(['future_ofi_bucket','future_obi_bucket','spot_ofi_bucket','spot_obi_bucket']).agg([
            (pl.col('future_return').mean() * 10000).alias('avg_return'),
            ((pl.col('future_return') * 10000) > self.friction_cost_bps).mean().alias('winrate'),
            ((pl.col('future_return') * 10000) <= self.friction_cost_bps).mean().alias('lossrate'),
            (pl.when(pl.col('future_return') > 0).then(pl.col('future_return')).otherwise(None).mean() * 10000).alias('avg_win'),
            (pl.when(pl.col('future_return') <= 0).then(pl.col('future_return').abs()).otherwise(None).mean() * 10000).alias('avg_loss'),
            pl.len().alias('count')
        ]).with_columns([
            ((pl.col('winrate') * pl.col('avg_win')) - pl.col('lossrate') * pl.col('avg_loss')).alias('expectancy_bps')
        ]).filter(pl.col('ofi_bucket') is not None & pl.col('obi_bucket') is not None).with_columns([
            (pl.col('expectancy_bps') * pl.col('count').sqrt()).alias('score')
        ]).filter(pl.col('score').is_not_null()).sort('score',descending=True)

        for row in test_f_ofi_obil1.head(10).iter_rows(named=True):
            if row['future_ofi_bucket'] is not None and row['future_obi_bucket'] is not None and row['spot_ofi_bucket'] is not None and row['spot_obi_bucket'] is not None:
                self.logger.info(f"score: {row['score']} | expectancy: {row['expectancy_bps']} bps")
                self.logger.info(f"f_obi_l1_bucket: {row['future_obi_bucket']} | f_ofi_1s_bucket: {row['future_ofi_bucket']}")
                self.logger.info(f"s_obi_l1_bucket: {row['spot_obi_bucket']} | s_ofi_1s_bucket: {row['spot_ofi_bucket']}")
                self.logger.info(f"avg_return: {row['avg_return']:.5f} bps | winrate: {row['winrate']} | count: {row['count']}")
                self.logger.info("\n")

        self.logger.info(f"----------------------------")

        test_f_ofi_obil5 = df.with_columns([
            pl.col('future_ofi_1s').qcut(5).alias('ofi_bucket'),
            pl.col('future_obi_l5').qcut(5).alias('obi_bucket')
        ])

        test_f_ofi_obil5 = test_f_ofi_obil5.group_by(['ofi_bucket','obi_bucket']).agg([
            (pl.col('future_return').mean() * 10000).alias('avg_return'),
            (pl.col('future_return') > 0).mean().alias('winrate'),
            (pl.col('future_return') <= 0).mean().alias('lossrate'),
            (pl.when(pl.col('future_return') > 0).then(pl.col('future_return')).otherwise(None).mean() * 10000).alias('avg_win'),
            (pl.when(pl.col('future_return') <= 0).then(pl.col('future_return').abs()).otherwise(None).mean() * 10000).alias('avg_loss'),
            pl.len().alias('count')
        ]).with_columns([
            ((pl.col('winrate') * pl.col('avg_win')) - pl.col('lossrate') * pl.col('avg_loss')).alias('expectancy_bps')
        ]).filter(pl.col('ofi_bucket') is not None & pl.col('obi_bucket') is not None).with_columns([
            (pl.col('expectancy_bps') * pl.col('count').log()).alias('score')
        ]).filter(pl.col('score') is not None).sort('score',descending=True)

        for row in test_f_ofi_obil5.head(10).iter_rows(named=True):
            if row['obi_bucket'] is not None and row['ofi_bucket'] is not None:
                self.logger.info(f"score: {row['score']} | expectancy: {row['expectancy_bps']} bps")
                self.logger.info(f"f_obi_l5_bucket: {row['obi_bucket']} | f_ofi_1s_bucket: {row['ofi_bucket']}")
                self.logger.info(f"avg_return: {row['avg_return']:.5f} bps | winrate: {row['winrate']} | count: {row['count']}")
                self.logger.info("\n")

        # for row in df.iter_rows(named=True):
        #     self.logger.info(f"ts:{row['timestamp']} | return:{row['future_return']:.5f} | bid_p_f:{row['bid_price_future']:.4f} | ask_p_f:{row['ask_price_future']:.4f} | bid_p_s:{row['bid_price_spot']:.4f} | ask_p_s:{row['ask_price_spot']:.4f}")
        #     self.logger.info(f"----f_ofi_1s:{row['future_ofi_1s']:.4f} | s_ofi_1s:{row['spot_ofi_1s']:.4f}")
        #     self.logger.info(f"----f_obi_l1:{row['future_obi_l1']:.4f} | f_obi_l3:{row['future_obi_l3']:.4f} | f_obi_l5:{row['future_obi_l5']:.4f} | f_obi_l10:{row['future_obi_l10']:.4f}")
        #     self.logger.info(f"----s_obi_l1:{row['spot_obi_l1']:.4f} | s_obi_l3:{row['spot_obi_l3']:.4f} | s_obi_l5:{row['spot_obi_l5']:.4f} | s_obi_l10:{row['spot_obi_l10']:.4f}")

        