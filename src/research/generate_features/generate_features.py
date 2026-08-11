import polars as pl
import numpy as np
from math import ceil

def generate_taker_features(
        df_orderbook_spot:pl.LazyFrame,
        df_orderbook_swap:pl.LazyFrame,
        df_trades_spot:pl.LazyFrame,
        df_trades_swap:pl.LazyFrame,
        df_mark_price:pl.LazyFrame,
        df_open_interest:pl.LazyFrame
    ) -> pl.LazyFrame:
    # 1. 聚合现货 Trade 数据（以 50ms 或者是对齐盘口时间戳为基准）
    # 这里演示按盘口时间戳就近拼接或者滚动聚合
    spot_trades_df_1s = _trades_ofi(df_trades_spot,ts=1000).rename({
        "signed_turnover": "spot_trade_flow_1s",
        "signed_amount": "spot_trade_amount_1s",
        "turnover": "spot_trade_turnover_1s",
        "trade_count": "spot_trade_count_1s",
    })
    spot_trades_df_2s = _trades_ofi(df_trades_spot,ts=2000).rename({
        "signed_turnover": "spot_trade_flow_2s",
        "signed_amount": "spot_trade_amount_2s",
        "turnover": "spot_trade_turnover_2s",
        "trade_count": "spot_trade_count_2s",
    })
    swap_trades_df_1s = _trades_ofi(df_trades_swap,ts=1000).rename({
        "signed_turnover": "swap_trade_flow_1s",
        "signed_amount": "swap_trade_amount_1s",
        "turnover": "swap_trade_turnover_1s",
        "trade_count": "swap_trade_count_1s",
    })
    swap_trades_df_2s = _trades_ofi(df_trades_swap,ts=2000).rename({
        "signed_turnover": "swap_trade_flow_2s",
        "signed_amount": "swap_trade_amount_2s",
        "turnover": "swap_trade_turnover_2s",
        "trade_count": "swap_trade_count_2s",
    })
    spot_ob_df_1s = _orderbook_ofi(df_orderbook_spot,'spot',ts=1000).rename({
        "ofi":"spot_ob_ofi_1s"
    })
    spot_ob_df_2s = _orderbook_ofi(df_orderbook_spot,'spot',ts=2000).rename({
        "ofi":"spot_ob_ofi_2s"
    })
    swap_ob_df_1s = _orderbook_ofi(df_orderbook_swap,'swap',ts=1000).rename({
        "ofi":"swap_ob_ofi_1s"
    })
    swap_ob_df_2s = _orderbook_ofi(df_orderbook_swap,'swap',ts=2000).rename({
        "ofi":"swap_ob_ofi_2s"
    })
    
    df_features = df_orderbook_swap.sort('timestamp').join_asof(
        df_orderbook_spot.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    ).join_asof(
        spot_ob_df_1s.sort('timestamp'),
        on="timestamp",
        strategy="backward"
    ).join_asof(
        spot_ob_df_2s.sort('timestamp'),
        on="timestamp",
        strategy="backward"
    ).join_asof(
        swap_ob_df_1s.sort('timestamp'),
        on="timestamp",
        strategy="backward"
    ).join_asof(
        swap_ob_df_2s.sort('timestamp'),
        on="timestamp",
        strategy="backward"
    ).join_asof(
        spot_trades_df_1s.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    ).join_asof(
        spot_trades_df_2s.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    ).join_asof(
        swap_trades_df_1s.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    ).join_asof(
        swap_trades_df_2s.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    ).join_asof(
        df_mark_price.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    ).join_asof(
        df_open_interest.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    )
    # 计算obi深度 5，10，20depth
    df_features = df_features.with_columns([
        _calculate_obi("swap",1),
        _calculate_obi("swap",5),
        _calculate_obi("spot",1),
        _calculate_obi("spot",5),
    ])
    df_features = df_features.with_columns([
        pl.col("bid_prices_swap").list.get(0).alias("best_bid"),
        pl.col("ask_prices_swap").list.get(0).alias("best_ask")
    ])

    df_features = _add_return_features(
        df=df_features,
        windows_minutes=(1,5,15,60)
    )

    df_features = df_features.with_columns([
        pl.col("return_bps_1m").abs().alias("abs_return_bps_1m"),
        pl.col("return_bps_5m").abs().alias("abs_return_bps_5m"),
        pl.col("return_bps_15m").abs().alias("abs_return_bps_15m"),
        pl.col("return_bps_60m").abs().alias("abs_return_bps_60m")
    ])

    df_features = _add_realized_volatility_features(
        df=df_features,
        windows_minutes=(1,5,15,60)
    )

    return df_features.select([
        "timestamp",
        "realized_vol_bps_1m",
        "realized_vol_bps_5m",
        "realized_vol_bps_15m",
        "realized_vol_bps_60m",
        "return_bps_1m",
        "return_bps_5m",
        "return_bps_15m",
        "return_bps_60m",
        "abs_return_bps_1m",
        "abs_return_bps_5m",
        "abs_return_bps_15m",
        "abs_return_bps_60m",
        "spot_trade_flow_1s",
        "spot_trade_flow_2s",
        "swap_trade_flow_1s",
        "swap_trade_flow_2s",
        "spot_ob_ofi_1s",
        "spot_ob_ofi_2s",
        "swap_ob_ofi_1s",
        "swap_ob_ofi_2s",
        "swap_obi_l5",
        "swap_obi_l1",
        "spot_obi_l5",
        "spot_obi_l1",
        "mark_price",
        "open_interest_amount",
        "micro_price_swap",
        "mid_price_swap",
        "mid_price_spot",
        "spread_swap",
        "best_bid",
        "best_ask",
    ])

def _add_realized_volatility_features(
    df:pl.LazyFrame,
    windows_minutes:tuple[int,...]=(1,5,15,60),
    min_coverage_ratio:float=0.98
) -> pl.LazyFrame:
    one_seconds_prices = (
        df.select([
            "timestamp",
            "mid_price_swap",
        ])
        .with_columns([
            pl.col("timestamp").cast(pl.Datetime("ms"))
        ])
        .sort("timestamp")
        .group_by_dynamic(
            index_column="timestamp",
            every="1s",
            period="1s",
            closed="right",
            label="right"
        )
        .agg([
            pl.col("mid_price_swap").last()
        ])
        .with_columns([
            (pl.col("mid_price_swap").log() - pl.col("mid_price_swap").shift(1).log()).alias("_log_return_1s")
        ])
        .with_columns([
            pl.col("_log_return_1s").pow(2).alias("_squared_log_return_1s")
        ])
    )

    volatility_colums = []

    for minutes in windows_minutes:
        column_name = f"realized_vol_bps_{minutes}m"

        one_seconds_prices = one_seconds_prices.with_columns([
            (pl.col("_squared_log_return_1s").rolling_sum_by(
                by="timestamp",
                window_size=f"{minutes}m",
                min_samples=ceil(minutes * 60 * min_coverage_ratio),
                closed="right"
            ).clip(lower_bound=0.0).sqrt() * 10000).alias(column_name)
        ])

        volatility_colums.append(column_name)

    volatility_lookup = one_seconds_prices.select([
        pl.col("timestamp").cast(pl.Int64),
        *volatility_colums
    ]).sort("timestamp")

    return df.sort("timestamp").join_asof(
        volatility_lookup,
        on="timestamp",
        strategy="backward"
    )

def _add_return_features(
    df:pl.LazyFrame,
    windows_minutes:tuple[int,...]=(1,5,15,60)
) -> pl.LazyFrame:
    prices = df.select([
        "timestamp",
        "mid_price_swap",
    ])

    result = df

    for minutes in windows_minutes:
        past_price_column = f"_mid_price_swap_{minutes}m_ago"

        past_prices = prices.select([
            (pl.col("timestamp") + minutes * 60_000).alias("timestamp"),
            pl.col("mid_price_swap").alias(past_price_column)
        ]).sort("timestamp")

        result = result.sort("timestamp").join_asof(
            past_prices,
            on="timestamp",
            strategy="backward"
        ).with_columns([
            ((pl.col("mid_price_swap") / pl.col(past_price_column) - 1) * 10_000).alias(f"return_bps_{minutes}m")
        ]).drop(past_price_column)

    return result

def _orderbook_ofi(df:pl.LazyFrame,type_name:str,ts=1000):
    return (
        df.with_columns([pl.col("timestamp").cast(pl.Datetime("ms"))])
        .sort("timestamp")
        .group_by_dynamic(
            index_column="timestamp",
            every=f"{ts}ms",
            period=f"{ts}ms",
            closed="right",
            label="right"
        )
        .agg([
            _calculate_ob_ofi(type_name)
        ])
        .with_columns([pl.col("timestamp").cast(pl.Int64)])
    )

def _calculate_ob_ofi(type_name:str) -> pl.Expr:
    bid = pl.when(pl.col(f"bid_prices_{type_name}").list.get(0) > pl.col(f"bid_prices_{type_name}").shift(1).list.get(0)).then(pl.col(f"bid_volumes_{type_name}").list.get(0)).when(pl.col(f"bid_prices_{type_name}").list.get(0) == pl.col(f"bid_prices_{type_name}").shift(1).list.get(0)).then(pl.col(f"bid_volumes_{type_name}").list.get(0) - pl.col(f"bid_volumes_{type_name}").shift(1).list.get(0)).otherwise(-pl.col(f"bid_volumes_{type_name}").shift(1).list.get(0)).sum()

    ask = pl.when(pl.col(f"ask_prices_{type_name}").list.get(0) < pl.col(f"ask_prices_{type_name}").shift(1).list.get(0)).then(-pl.col(f"ask_volumes_{type_name}").list.get(0)).when(pl.col(f"ask_prices_{type_name}").list.get(0) == pl.col(f"ask_prices_{type_name}").shift(1).list.get(0)).then(-(pl.col(f"ask_volumes_{type_name}").list.get(0) - pl.col(f"ask_volumes_{type_name}").shift(1).list.get(0))).otherwise(pl.col(f"ask_volumes_{type_name}").shift(1).list.get(0)).sum()

    return (bid + ask).alias("ofi")

def _trades_ofi(df:pl.LazyFrame,ts=1000):
    return (
        df.with_columns([
            pl.col("timestamp").cast(pl.Datetime("ms")),
            (pl.col("price") * pl.col("amount")).alias("turnover")
        ])
        .sort("timestamp")
        .group_by_dynamic(
            index_column="timestamp",
            every=f"{ts}ms",
            period=f"{ts}ms",
            closed="right",
            label="right"
        ).agg([
            pl.when(pl.col("side") == "buy").then(pl.col("turnover")).otherwise(-pl.col("turnover")).sum().alias("signed_turnover"),
            pl.when(pl.col("side") == "buy").then(pl.col("amount")).otherwise(-pl.col("amount")).sum().alias("signed_amount"),
            pl.col("turnover").sum().alias("turnover"),
            pl.len().alias("trade_count")
        ])
        .with_columns([pl.col("timestamp").cast(pl.Int64)])
    )

def _calculate_obi(type_name:str,depth:int) -> pl.Expr:
    bid = f'bid_volumes_{type_name}'
    ask = f'ask_volumes_{type_name}'
    return ((pl.col(bid).list.slice(0,depth).list.sum() - pl.col(ask).list.slice(0,depth).list.sum()) / (pl.col(bid).list.slice(0,depth).list.sum() + pl.col(ask).list.slice(0,depth).list.sum())).alias(f'{type_name}_obi_l{depth}')
