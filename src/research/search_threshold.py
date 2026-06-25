import itertools
import polars as pl

def eval_signal(df:pl.DataFrame,signal) -> dict | None:
    sub = df.filter(signal)
    count = len(sub)

    if count == 0: return None

    avg = sub["future_return"].mean()
    median = sub["future_return"].median()
    std = sub["future_return"].std()

    score = avg * (count ** 0.5)

    return {
        "count":count,
        "avg":avg,
        "median":median,
        "std":std,
        "score":score,
    }

def search_long_threshold(df:pl.DataFrame) -> pl.DataFrame:
    results = []

    grids = {
        "future_obi_l1_q": [0.95, 0.97, 0.98, 0.99],
        "spot_obi_l1_q": [0.90, 0.95, 0.97, 0.98],
        "future_ob_ofi_1s_q": [0.90, 0.95, 0.97],
        "future_ob_ofi_2s_q": [0.90, 0.95, 0.97],
        "future_trade_flow_1s_q": [0.90, 0.95, 0.97, 0.98],
        "future_trade_flow_2s_q": [0.90, 0.95, 0.97],
        "spot_trade_flow_1s_q": [0.90, 0.95, 0.97],
        "spot_trade_flow_2s_q": [0.90, 0.95, 0.97],
    }

    keys = list(grids.keys())

    for values in itertools.product(*(grids[k] for k in keys)):
        params = dict(zip(keys,values))

        signal = None

        for k,v in params.items():
            cond = df[k] > v
            signal = cond if signal is None else signal & cond

        r = eval_signal(df,signal)

        if r is None: continue

        if r["count"] < 20: continue

        results.append({
            **params,
            **r
        })

    if not results: return pl.DataFrame()

    return pl.DataFrame(results).sort("score",descending=True)