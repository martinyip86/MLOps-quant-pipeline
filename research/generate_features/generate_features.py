import polars as pl
import numpy as np

def generate_maker_features(
        df_orderbook_spot:pl.LazyFrame,
        df_orderbook_future:pl.LazyFrame,
        df_trades_spot:pl.LazyFrame,
        df_trades_future:pl.LazyFrame,
        df_mark_price:pl.LazyFrame,
        df_open_interest:pl.LazyFrame
    ) -> pl.LazyFrame:
    # 1. 聚合现货 Trade 数据（以 50ms 或者是对齐盘口时间戳为基准）
    # 这里演示按盘口时间戳就近拼接或者滚动聚合
    df_spot_agg = df_trades_spot.with_columns(pl.col('timestamp').cast(pl.Datetime("ms"))).group_by_dynamic('timestamp',every='100ms').agg([
        pl.when(pl.col('is_taker_buyer') == 1).then(pl.col('amount')).otherwise(-pl.col('amount')).sum().alias('spot_net_buy_vol'),
        pl.col('amount').sum().fill_null(0.0).alias('spot_total_vol')
    ]).with_columns([
        pl.col('spot_net_buy_vol').rolling_sum(20).alias('spot_ofi_2s'),
        pl.col('spot_total_vol').rolling_mean(10).fill_null(0.0).alias('spot_vol_ma'),
        pl.col('timestamp').cast(pl.Int64)
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
        pl.col('future_net_buy_vol').rolling_sum(window_size=10).alias('future_ofi_1s'),
        pl.col('future_net_buy_vol').rolling_sum(window_size=50).alias('future_ofi_5s')
    ])

    df_oi_agg = df_open_interest.with_columns([
        pl.col('open_interest_amount').diff().alias('oi_diff')
    ]).with_columns([
        pl.col('oi_diff').rolling_sum(10).fill_null(0.0).alias('oi_momentum')
    ])

    df_features = df_orderbook_future.sort('timestamp').join_asof(
        df_orderbook_spot.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    ).join_asof(
        df_spot_agg.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    ).join_asof(
        df_future_agg.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    ).join_asof(
        df_mark_price.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    ).join_asof(
        df_oi_agg.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    )
    # 计算obi深度 5，10，20depth
    df_features = df_features.with_columns([
        ((pl.col('bid_volumes_spot').list.slice(0,20).list.sum() - pl.col('ask_volumes_spot').list.slice(0,20).list.sum()) /
        (pl.col('bid_volumes_spot').list.slice(0,20).list.sum() + pl.col('ask_volumes_spot').list.slice(0,20).list.sum() + 1e-8)).alias('spot_obi_l20'),
        ((pl.col('bid_volumes_spot').list.slice(0,5).list.sum() - pl.col('ask_volumes_spot').list.slice(0,5).list.sum()) /
        (pl.col('bid_volumes_spot').list.slice(0,5).list.sum() + pl.col('ask_volumes_spot').list.slice(0,5).list.sum() + 1e-8)).alias('spot_obi_l5'),
        ((pl.col('bid_volumes_spot').list.slice(0,10).list.sum() - pl.col('ask_volumes_spot').list.slice(0,10).list.sum()) /
        (pl.col('bid_volumes_spot').list.slice(0,10).list.sum() + pl.col('ask_volumes_spot').list.slice(0,10).list.sum() + 1e-8)).alias('spot_obi_l10'),
        ((pl.col('bid_volumes_future').list.slice(0,20).list.sum() - pl.col('ask_volumes_future').list.slice(0,20).list.sum()) /
        (pl.col('bid_volumes_future').list.slice(0,20).list.sum() + pl.col('ask_volumes_future').list.slice(0,20).list.sum() + 1e-8)).alias('future_obi_l20'),
        ((pl.col('bid_volumes_future').list.slice(0,5).list.sum() - pl.col('ask_volumes_future').list.slice(0,5).list.sum()) /
        (pl.col('bid_volumes_future').list.slice(0,5).list.sum() + pl.col('ask_volumes_future').list.slice(0,5).list.sum() + 1e-8)).alias('future_obi_l5'),
        ((pl.col('bid_volumes_future').list.slice(0,10).list.sum() - pl.col('ask_volumes_future').list.slice(0,10).list.sum()) /
        (pl.col('bid_volumes_future').list.slice(0,10).list.sum() + pl.col('ask_volumes_future').list.slice(0,10).list.sum() + 1e-8)).alias('future_obi_l10')
    ])
    # 计算期现基差
    df_features = df_features.with_columns([
        (pl.col('mid_price_future') - pl.col('mid_price_spot')).alias('future_spot_basis'),
        ((pl.col('mid_price_future') - pl.col('mid_price_spot')) / pl.col('mid_price_spot') * 10000).alias('future_spot_basis_bp')
    ])

    # --- 6. 🌟 策略切换核心逻辑：构建 Taker 与 Maker 的触发标签 ---
    # 填充可能因为没有成交而产生的 Null 值，防止基差和动能报出几万的死数
    df_features = df_features.with_columns([
        pl.col('future_ofi_1s').fill_null(0.0),
        pl.col('future_ofi_5s').fill_null(0.0),
        pl.col('future_spot_basis').fill_null(0.0),
        pl.col('future_spot_basis_bp').fill_null(0.0),
        pl.col('oi_momentum').fill_null(0.0)
    ])

    # 动态定义什么是“大幅度波动”（阈值需要你根据回测调整，这里先给个示例）
    # 当 1秒 动能极大，或者 OBI 极度倾斜时，标记为趋势爆发
    df_features = df_features.with_columns([
        pl.when(
            (pl.col('future_ofi_1s') > 30.0) &          # 期货强力主动买盘
            (pl.col('future_obi_l5') > 0.75) &          # 盘口买方严重压制（看L5更灵敏）
            (pl.col('spot_ofi_2s') > 15.0) &            # 现货必须有真金白银同步买入
            (pl.col('spot_obi_l5') > 0.50)
        ).then(1).otherwise(0).alias('signal_long'),
        pl.when(
            (pl.col('future_ofi_1s') < -30.0) & 
            (pl.col('future_obi_l5') < -0.75) &         # 盘口卖方严重压制
            (pl.col('spot_ofi_2s') < -15.0) & 
            (pl.col('spot_obi_l5') < -0.50) &            # 现货下方有挂单支撑
            (pl.col('oi_momentum') > 0)
        ).then(-1).otherwise(0).alias('signal_short')
    ])

    df_features = df_features.with_columns([
        (pl.col('mid_price_future') - pl.col('mark_price')).alias('premium_discount'),
        pl.col('future_spot_basis').rolling_mean(50).fill_null(strategy='forward').alias('basis_ma'),
        (pl.col('micro_price_spot') - pl.col('mid_price_spot')).alias('microprice_dev_spot'),
        pl.col('spread_spot').rolling_mean(20).alias('spread_ma_spot'),
        (pl.col('spread_spot') / pl.col('spread_spot').rolling_mean(100)).alias('spot_spread_zscore'),
        (pl.col('future_ofi_1s') / pl.col('mid_price_spot') * 10000).alias('ofi_bps_1s')
    ])
    return df_features.select([
        'timestamp',
        'spot_ofi_2s',
        'future_ofi_1s',
        'future_ofi_5s',
        'spot_obi_l20',
        'spot_obi_l10',
        'spot_obi_l5',
        'future_obi_l20',
        'future_obi_l10',
        'future_obi_l5',
        'future_spot_basis',
        'future_spot_basis_bp',
        'signal_long',
        'signal_short',
        'premium_discount',
        'basis_ma',
        'microprice_dev_spot',
        'spread_ma_spot',
        'spot_spread_zscore',
        'ofi_bps_1s',
        'oi_momentum',
        'mark_price',
        pl.col('bid_prices_spot').list.get(0).alias('bid_price_spot'),
        pl.col('ask_prices_spot').list.get(0).alias('ask_price_spot'),
        pl.col('bid_prices_future').list.get(0).alias('bid_price_future'),
        pl.col('ask_prices_future').list.get(0).alias('ask_price_future'),
        'spread_future',
        'mid_price_future',
        'buy_impact_bps_future'
    ])

def _calculate_obi(type_name:str,depth:int) -> pl.Expr:
    bid = f'bid_volumes_{type_name}'
    ask = f'ask_volumes_{type_name}'
    return ((pl.col(bid).list.slice(0,depth).list.sum() - pl.col(ask).list.slice(0,depth).list.sum()) / (pl.col(bid).list.slice(0,depth).list.sum() + pl.col(ask).list.slice(0,depth).list.sum())).alias(f'{type_name}_obi_l{depth}')

def generate_features_v1(
    df_orderbook_spot:pl.LazyFrame,
    df_orderbook_future:pl.LazyFrame,
    df_trades_spot:pl.LazyFrame,
    df_trades_future:pl.LazyFrame,
    df_mark_price:pl.LazyFrame,
    df_open_interest:pl.LazyFrame
) -> pl.LazyFrame:
    trades_future_data = df_trades_future.with_columns([
        pl.when(pl.col('side') == 'buy').then(pl.col('amount')).otherwise(-pl.col('amount')).alias('flow')
    ]).with_columns([
        pl.col('flow').cum_sum().alias('future_cum_ofi')
    ])

    trades_spot_data = df_trades_spot.with_columns([
        pl.when(pl.col('side') == 'buy').then(pl.col('amount')).otherwise(-pl.col('amount')).alias('flow')
    ]).with_columns([
        pl.col('flow').cum_sum().alias('spot_cum_ofi')
    ])

    df_features = df_orderbook_future.join_asof(
        df_orderbook_spot,
        on='timestamp',
        strategy='backward'
    ).join_asof(
        trades_future_data,
        on='timestamp',
        strategy='backward'
    ).join_asof(
        trades_spot_data,
        on='timestamp',
        strategy='backward'
    )

    df_features = df_features.with_columns([
        _calculate_obi('future',1),
        _calculate_obi('future',3),
        _calculate_obi('future',5),
        _calculate_obi('future',10),
        _calculate_obi('spot',1),
        _calculate_obi('spot',3),
        _calculate_obi('spot',5),
        _calculate_obi('spot',10)
    ])

    df_features = df_features.with_columns([
        (pl.col('future_cum_ofi') - pl.col('future_cum_ofi').shift(10)).alias('future_ofi_1s'),
        (pl.col('spot_cum_ofi') - pl.col('spot_cum_ofi').shift(10)).alias('spot_ofi_1s')
    ])

    df_features = df_features.with_columns([
        ((pl.col('mid_price_future').shift(-10000) - pl.col('mid_price_future')) / pl.col('mid_price_future')).alias('future_return')
    ])
    
    return df_features.select([
        'timestamp',
        pl.col('bid_prices_future').list.get(0).alias('bid_price_future'),
        pl.col('ask_prices_future').list.get(0).alias('ask_price_future'),
        pl.col('bid_prices_spot').list.get(0).alias('bid_price_spot'),
        pl.col('ask_prices_spot').list.get(0).alias('ask_price_spot'),
        'future_ofi_1s',
        'spot_ofi_1s',
        'future_obi_l1',
        'future_obi_l3',
        'future_obi_l5',
        'future_obi_l10',
        'spot_obi_l1',
        'spot_obi_l3',
        'spot_obi_l5',
        'spot_obi_l10',
        'future_return',
    ])