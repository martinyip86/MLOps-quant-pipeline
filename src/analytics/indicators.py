import polars as pl
import numpy as np

def calc_vamp_expr(depth=5) -> pl.Expr:
    vamp_bid = (pl.col('bid_prices').list.slice(0,depth) * pl.col('bid_volumes').list.slice(0,depth)).list.sum() / pl.col('bid_volumes').list.slice(0,depth).list.sum()
    vamp_ask = (pl.col('ask_prices').list.slice(0,depth) * pl.col('ask_volumes').list.slice(0,depth)).list.sum() / pl.col('ask_volumes').list.slice(0,depth).list.sum()

    return ((vamp_bid + vamp_ask) / 2).alias('vamp')

def calc_ofi_expr() -> pl.Expr:
    p_b = pl.col('bid_prices').list.get(0)
    v_b = pl.col('bid_volumes').list.get(0)
    p_a = pl.col('ask_prices').list.get(0)
    v_a = pl.col('ask_volumes').list.get(0)

    db = (pl.when(p_b > p_b.shift(1)).then(v_b).when(p_b==p_b.shift(1)).then(v_b - v_b.shift(1)).otherwise(-v_b.shift(1)))
    da = (pl.when(p_a < p_a.shift(1)).then(v_a).when(p_a == p_a.shift(1)).then(v_a - v_a.shift(1)).otherwise(-v_a.shift(1)))

    return (db - da).alias('ofi')

def calc_rsi_expr(window:int=14) -> pl.Expr:
    delta = pl.col('close').diff()
    gain = delta.clip(lower_bound=0).rolling_mean(window)
    loss = (-delta.clip(upper_bound=0)).rolling_mean(window)
    rs = gain / (loss + 1e-8)
    return (100 - (100 / (1 + rs))).alias('rsi')

def calc_macd_expr() -> pl.Expr:
    ema_fast = pl.col('close').ewm_mean(span=12,adjust=False)
    ema_slow = pl.col('close').ewm_mean(span=26,adjust=False)
    macd = ema_fast - ema_slow
    macd_signal = macd.ewm_mean(span=9,adjust=False)
    return (macd - macd_signal).alias('macd_hist')

def calc_ema50_expr() -> pl.Expr:
    return pl.col('close').ewm_mean(span=50,adjust=False).alias('ema50')

def calc_volume_ma_expr() -> pl.Expr:
    return pl.col('volume').rolling_mean(window_size=20).alias('vol_ma')

def calc_atr_expr() -> pl.Expr:
    hl = pl.col('height') - pl.col('low')
    hc = (pl.col('height') - pl.col('close').shift(1)).abs()
    lc = (pl.col('low') - pl.col('close').shift(1)).abs()
    tr = pl.max_horizontal(hl,hc,lc)
    return tr.rolling_mean(window_size=14).alias('atr')