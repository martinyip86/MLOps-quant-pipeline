from research.train_maker_as import TrainMakerAS
from research.train_taker_as import TrainTakerAS

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
    
    def generate_maker_features(self,df_orderbook:pl.LazyFrame,df_trades_spot:pl.LazyFrame,df_trades_future:pl.LazyFrame) -> pl.LazyFrame:
        # 1. 聚合现货 Trade 数据（以 50ms 或者是对齐盘口时间戳为基准）
        # 这里演示按盘口时间戳就近拼接或者滚动聚合
        df_spot_agg = df_trades_spot.with_columns(pl.col('timestamp').cast(pl.Datetime("ms"))).group_by_dynamic('timestamp',every='100ms').agg([
            pl.when(pl.col('is_taker_buyer') == 1).then(pl.col('amount')).otherwise(-pl.col('amount')).sum().alias('spot_net_buy_vol'),
            pl.col('amount').sum().alias('spot_total_vol')
        ]).with_columns([
            pl.col('spot_net_buy_vol').rolling_sum(20).alias('ofi_1s'),
            pl.col('spot_total_vol').rolling_mean(10).alias('spot_vol_ma'),
            pl.col('timestamp').cast(pl.Int64)
        ]).with_columns([
            pl.col('ofi_1s').fill_null(strategy='forward'),
            pl.col('spot_vol_ma').fill_null(strategy='forward')
        ])

        df_future_agg = df_trades_future.with_columns(pl.col('timestamp').cast(pl.Datetime("ms"))).group_by_dynamic('timestamp',every='100ms').agg([
            pl.col('price').last().alias('future_avg_price'),
            pl.when(pl.col('side') == 'buy').then(pl.col('amount')).otherwise(-pl.col('amount')).sum().alias('future_net_buy_vol')
        ]).with_columns([
            pl.col('timestamp').cast(pl.Int64)
        ])

        # --- 3. 核心修正：在时间规整的期货表上计算“真正的 1秒(10个100ms) 动能” ---
        # 这样能确保 momentum 是严格时间意义上的滑动窗口，不会受盘口刷新频率干扰
        df_future_agg = df_future_agg.with_columns([
            pl.col('future_net_buy_vol').rolling_sum(window_size=10).alias('future_momentum_1s'),
            pl.col('future_net_buy_vol').rolling_sum(window_size=50).alias('future_momentum_5s')
        ])

        df_features = df_orderbook.sort('timestamp').join_asof(
            df_spot_agg.sort('timestamp'),
            on='timestamp',
            strategy='backward'
        ).join_asof(
            df_future_agg.sort('timestamp'),
            on='timestamp',
            strategy='backward'
        )

        df_features = df_features.with_columns([
            ((pl.col('bid_volumes').list.slice(0,20).list.sum() - pl.col('ask_volumes').list.slice(0,20).list.sum()) /
            (pl.col('bid_volumes').list.slice(0,20).list.sum() + pl.col('ask_volumes').list.slice(0,20).list.sum() + 1e-8)).alias('obi_l20'),
            ((pl.col('bid_volumes').list.slice(0,5).list.sum() - pl.col('ask_volumes').list.slice(0,5).list.sum()) /
            (pl.col('bid_volumes').list.slice(0,5).list.sum() + pl.col('ask_volumes').list.slice(0,5).list.sum() + 1e-8)).alias('obi_l5'),
            ((pl.col('bid_volumes').list.slice(0,10).list.sum() - pl.col('ask_volumes').list.slice(0,10).list.sum()) /
            (pl.col('bid_volumes').list.slice(0,10).list.sum() + pl.col('ask_volumes').list.slice(0,10).list.sum() + 1e-8)).alias('obi_l10'),
            (pl.col('future_avg_price') - pl.col('mid_price')).alias('future_spot_basis')
        ])

        # --- 6. 🌟 策略切换核心逻辑：构建 Taker 与 Maker 的触发标签 ---
        # 填充可能因为没有成交而产生的 Null 值，防止基差和动能报出几万的死数
        df_features = df_features.with_columns([
            pl.col('future_momentum_1s').fill_null(0.0),
            pl.col('future_momentum_5s').fill_null(0.0),
            pl.col('future_spot_basis').fill_null(0.0)
        ])

        # 动态定义什么是“大幅度波动”（阈值需要你根据回测调整，这里先给个示例）
        # 当 1秒 动能极大，或者 OBI 极度倾斜时，标记为趋势爆发
        df_features = df_features.with_columns([
            pl.when(
                ((pl.col('future_momentum_1s').abs() > 50.0) |  # 期货 1 秒内净买卖量超 50 币
                (pl.col('obi_l20').abs() > 0.90)) &             # 盘口极度单边压土机
                (pl.col('spot_total_vol') > pl.col('spot_vol_ma'))
            ).then(True).otherwise(False).alias('is_trending_regime')
        ])

        df_features = df_features.with_columns([
            pl.col('future_spot_basis').rolling_mean(50).fill_null(strategy='forward').alias('basis_ma'),
            ((pl.col('future_avg_price') - pl.col('mid_price')) / pl.col('mid_price')).alias('basis_pct'),
            (pl.col('micro_price') - pl.col('mid_price')).alias('microprice_dev'),
            pl.col('spread').rolling_mean(20).alias('spread_ma'),
            (pl.col('spread') / pl.col('spread').rolling_mean(100)).alias('spread_zscore'),
            (pl.col('future_momentum_1s') / pl.col('mid_price') * 10000).alias('mom_bps_1s')
        ])
        return df_features

    def main(self):
        orderbook = self._get_data_lake(
            exchange_id=self.exchange_id,
            mkt_type='spot',
            symbol=self.symbol,
            data_type='orderbook',
            days=1
        ).select(['timestamp','bid_prices','bid_volumes','ask_prices','ask_volumes','micro_price','spread','mid_price','sim_buy_price_avg','buy_impact_bps']).sort('timestamp')
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

        # trades = trades.group_by_dynamic(index_column='timestamp',every='100i').agg([
        #     pl.col('amount').sum().alias('inverval_vol'),
        #     pl.col('price').last().alias('last_price'),
        #     pl.col('amount').filter(pl.col('is_taker_buyer') == True).sum().alias('taker_buy_vol'),
        #     pl.col('amount').filter(pl.col('is_taker_buyer') == False).sum().alias('taker_sell_vol'),
        #     pl.col('price').mean().alias('twap'),
        #     ((pl.col('price') * pl.col('amount')).sum() / (pl.col('amount').sum() + 1e-8)).alias('vwap'),
        #     pl.col('price').median().alias('prcei_median')
        # ]).with_columns([
        #     ((pl.col('taker_buy_vol') - pl.col('taker_sell_vol')) / pl.col('inverval_vol') + 1e-8).alias('ofi')
        # ])

        # lz = orderbook.join_asof(trades,on='timestamp',strategy='backward')

        # funding_rate = self._get_data_lake(
        #     exchange_id=self.exchange_id,
        #     mkt_type='future',
        #     symbol=self.symbol,
        #     data_type='funding_rate',
        #     days=1
        # ).select(['timestamp','funding_rate']).sort('timestamp')
        # lz = lz.join_asof(funding_rate,on='timestamp',strategy='backward')

        # df = lz.collect()

        orderbook = self.generate_maker_features(
            df_orderbook=orderbook,
            df_trades_spot=trades_spot,
            df_trades_future=trades_future
        )

        maker_model = TrainTakerAS()
        pnl = maker_model.run_taker_backtest(df_orderbook=orderbook.collect())
        

if __name__ == '__main__':
    obj = TrainAlpha()
    obj.main()