from src.storage.clickhouse.client import ch_manager
import polars as pl

def main():
    ch = ch_manager.connect('HK_HOST')

    sql = """
        SELECT * FROM market_data.trades_spot LIMIT 100
    """

    arrow = ch.query_arrow(sql)
    df = pl.from_arrow(arrow)
    print(df.head(10))

if __name__ == '__main__':
    main()