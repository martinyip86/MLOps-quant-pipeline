import polars as pl

def bucket_test(df:pl.DataFrame):
    return df.with_columns([
        ((pl.col("future_obi_l1") * 10).floor() / 10).alias("obi_bucket")
    ]).group_by("obi_bucket").agg([
        pl.mean("future_return").alias("avg_return"),
        pl.len()
    ]).sort("obi_bucket")