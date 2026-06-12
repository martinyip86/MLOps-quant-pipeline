from src.storage.clickhouse.client import ch_manager
import polars as pl

import time

def main():
    start_ms = int(time.time() * 1000)
    ch = ch_manager.connect('HK_HOST')

    sql = """
        SELECT * FROM market_data.trades_spot LIMIT 100
    """

    arrow = ch.query_arrow(sql)
    df = pl.from_arrow(arrow)
    print(df.head(10))
    end_ms = int(time.time() * 1000)
    used_ts = int(end_ms - start_ms)
    print(f"used ts: {used_ts}")


if __name__ == '__main__':
    main()