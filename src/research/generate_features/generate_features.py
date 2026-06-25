import polars as pl
import numpy as np

def generate_taker_features(
        df_orderbook_spot:pl.LazyFrame,
        df_orderbook_future:pl.LazyFrame,
        df_trades_spot:pl.LazyFrame,
        df_trades_future:pl.LazyFrame,
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
    future_trades_df_1s = _trades_ofi(df_trades_future,ts=1000).rename({
        "signed_turnover": "future_trade_flow_1s",
        "signed_amount": "future_trade_amount_1s",
        "turnover": "future_trade_turnover_1s",
        "trade_count": "future_trade_count_1s",
    })
    future_trades_df_2s = _trades_ofi(df_trades_future,ts=2000).rename({
        "signed_turnover": "future_trade_flow_2s",
        "signed_amount": "future_trade_amount_2s",
        "turnover": "future_trade_turnover_2s",
        "trade_count": "future_trade_count_2s",
    })
    spot_ob_df_1s = _orderbook_ofi(df_orderbook_spot,'spot',ts=1000).rename({
        "ofi":"spot_ob_ofi_1s"
    })
    spot_ob_df_2s = _orderbook_ofi(df_orderbook_spot,'spot',ts=2000).rename({
        "ofi":"spot_ob_ofi_2s"
    })
    future_ob_df_1s = _orderbook_ofi(df_orderbook_future,'future',ts=1000).rename({
        "ofi":"future_ob_ofi_1s"
    })
    future_ob_df_2s = _orderbook_ofi(df_orderbook_future,'future',ts=2000).rename({
        "ofi":"future_ob_ofi_2s"
    })
    
    df_features = df_orderbook_future.sort('timestamp').join_asof(
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
        future_ob_df_1s.sort('timestamp'),
        on="timestamp",
        strategy="backward"
    ).join_asof(
        future_ob_df_2s.sort('timestamp'),
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
        future_trades_df_1s.sort('timestamp'),
        on='timestamp',
        strategy='backward'
    ).join_asof(
        future_trades_df_2s.sort('timestamp'),
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
        _calculate_obi("future",1),
        _calculate_obi("future",5),
        _calculate_obi("spot",1),
        _calculate_obi("spot",5),
    ])
    df_features = df_features.with_columns([
        pl.col("bid_prices_future").list.get(0).alias("best_bid"),
        pl.col("ask_prices_future").list.get(0).alias("best_ask")
    ])
    return df_features.select([
        'timestamp',
        'spot_trade_flow_1s',
        'spot_trade_flow_2s',
        'future_trade_flow_1s',
        'future_trade_flow_2s',
        'spot_ob_ofi_1s',
        'spot_ob_ofi_2s',
        'future_ob_ofi_1s',
        'future_ob_ofi_2s',
        'future_obi_l5',
        'future_obi_l1',
        'spot_obi_l5',
        'spot_obi_l1',
        'mark_price',
        'open_interest_amount',
        'micro_price_future',
        'mid_price_future',
        'mid_price_spot',
        'spread_future',
        'best_bid',
        'best_ask'
    ])

def _orderbook_ofi(df:pl.LazyFrame,type_name:str,ts=1000):
    return (
        df.with_columns([pl.col("timestamp").cast(pl.Datetime("ms"))])
        .sort("timestamp")
        .group_by_dynamic(
            index_column="timestamp",
            every=f"{ts}ms",
            period=f"{ts}ms",
            closed="right"
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
            closed="right"
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
