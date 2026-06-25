import polars as pl

def generate_label(df:pl.DataFrame,horizon_ms=1000,tick_ms=100):
    shift_n = horizon_ms // tick_ms
    print(f"shift: {shift_n}")

    return df.with_columns([
        ((pl.col("mid_price_future").shift(-shift_n) / pl.col("mid_price_future") - 1) * 10000).alias(f"future_return")
    ])