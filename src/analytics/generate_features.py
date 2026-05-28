import polars as pl

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
    # df_spot_agg = df_trades_spot.with_columns(pl.col('timestamp').cast(pl.Datetime("ms"))).group_by_dynamic('timestamp',every='100ms').agg([
    #     pl.when(pl.col('is_taker_buyer') == 1).then(pl.col('amount')).otherwise(-pl.col('amount')).sum().alias('spot_net_buy_vol'),
    #     pl.col('amount').sum().fill_null(0.0).alias('spot_total_vol')
    # ]).with_columns([
    #     pl.col('timestamp').cast(pl.Int64)
    # ])
    df_spot_trades_processed = df_trades_spot.with_columns([
        pl.when(pl.col('is_taker_buyer') == 1).then(pl.col('amount')).otherwise(-pl.col('amount')).alias('raw_spot_net_buy_vol'),
        pl.col('amount').alias('raw_spot_total_vol')
    ]).select(['timestamp','raw_spot_net_buy_vol','raw_spot_total_vol'])

    # df_future_agg = df_trades_future.with_columns(pl.col('timestamp').cast(pl.Datetime("ms"))).group_by_dynamic('timestamp',every='100ms').agg([
    #     pl.col('price').last().alias('future_avg_price'),
    #     pl.when(pl.col('side') == 'buy').then(pl.col('amount')).otherwise(-pl.col('amount')).sum().alias('future_net_buy_vol')
    # ]).with_columns([
    #     pl.col('timestamp').cast(pl.Int64)
    # ])
    df_futute_trades_processed = df_trades_future.with_columns([
        pl.when(pl.col('side') == 'buy').then(pl.col('amount')).otherwise(-pl.col('amount')).alias('raw_future_net_vol')
    ]).select(['timestamp','raw_future_net_vol'])

    df_features = df_orderbook_future.sort('timestamp').join_asof(
        df_orderbook_spot.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    ).join_asof(
        df_spot_trades_processed.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    ).join_asof(
        df_futute_trades_processed.sort('timestamp'),
        on='timestamp',
        strategy='backward',
        tolerance=100
    ).join_asof(
        df_mark_price.sort('timestamp'),
        on='timestamp',
        strategy='backward',
        tolerance=100
    ).join_asof(
        df_open_interest.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    )

    df_features = df_features.with_columns([
        pl.col('raw_spot_net_buy_vol').fill_null(0.0),
        pl.col('raw_spot_total_vol').fill_null(0.0),
        pl.col('raw_future_net_vol').fill_null(0.0),
        pl.col('open_interest_amount').fill_null(strategy='forward')
    ])

    df_features = df_features.with_columns([
        pl.col('raw_spot_net_buy_vol').rolling_sum(20).alias('spot_ofi_2s'),
        pl.col('raw_spot_total_vol').rolling_mean(10).alias('spot_vol_ma'),
        pl.col('raw_future_net_vol').rolling_sum(window_size=10).alias('future_ofi_1s'),
        pl.col('raw_future_net_vol').rolling_sum(window_size=50).alias('future_ofi_5s'),
        (pl.col('open_interest_amount') - pl.col('open_interest_amount').shift(10)).fill_null(0.0).alias('oi_momentum')
    ])

    # 计算obi深度 5，10，20depth
    df_features = df_features.with_columns([
        ((pl.col('bid_volumes_spot').list.slice(0,5).list.sum() - pl.col('ask_volumes_spot').list.slice(0,5).list.sum()) /
        (pl.col('bid_volumes_spot').list.slice(0,5).list.sum() + pl.col('ask_volumes_spot').list.slice(0,5).list.sum() + 1e-8)).alias('spot_obi_l5'),
        ((pl.col('bid_volumes_future').list.slice(0,5).list.sum() - pl.col('ask_volumes_future').list.slice(0,5).list.sum()) /
        (pl.col('bid_volumes_future').list.slice(0,5).list.sum() + pl.col('ask_volumes_future').list.slice(0,5).list.sum() + 1e-8)).alias('future_obi_l5')
    ])
    # 计算期现基差
    df_features = df_features.with_columns([
        (pl.col('mid_price_future') - pl.col('mid_price_spot')).alias('future_spot_basis'),
        # ((pl.col('mid_price_future') - pl.col('mid_price_spot')) / pl.col('mid_price_spot') * 10000).alias('future_spot_basis_bp')
    ])

    # --- 6. 🌟 策略切换核心逻辑：构建 Taker 与 Maker 的触发标签 ---
    # 填充可能因为没有成交而产生的 Null 值，防止基差和动能报出几万的死数
    df_features = df_features.with_columns([
        pl.col('future_ofi_1s').fill_null(0.0),
        pl.col('future_ofi_5s').fill_null(0.0),
        pl.col('future_spot_basis').fill_null(0.0),
        # pl.col('future_spot_basis_bp').fill_null(0.0),
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
        # pl.col('future_spot_basis').rolling_mean(50).fill_null(strategy='forward').alias('basis_ma'),
        # (pl.col('micro_price_spot') - pl.col('mid_price_spot')).alias('microprice_dev_spot'),
        # pl.col('spread_spot').rolling_mean(20).alias('spread_ma_spot'),
        # (pl.col('spread_spot') / pl.col('spread_spot').rolling_mean(100)).alias('spot_spread_zscore'),
        # (pl.col('future_ofi_1s') / pl.col('mid_price_spot') * 10000).alias('ofi_bps_1s')
    ])

    return df_features.select([
        'timestamp',
        pl.col('bid_prices_future').list.get(0).alias('bid_price_future'),
        pl.col('ask_prices_future').list.get(0).alias('ask_price_future'),
        'mid_price_future',
        'spread_future',
        'buy_impact_bps_future',
        'signal_long',
        'signal_short',
        'future_ofi_1s',
        'future_obi_l5',
        'spot_ofi_2s',
        'spot_obi_l5',
        'oi_momentum',
        'future_spot_basis',
        'premium_discount'
    ])