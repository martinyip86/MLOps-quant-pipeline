import requests
import os
import polars as pl
import time
import sys
from datetime import datetime,timezone,timedelta

def main():
    days = 6
    url = "https://api1.binance.com/api/v3/klines"
    end_time = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp() * 1000)
    start_time = end_time - days * 24 * 60 * 60 * 1000

    symbol = 'BTC/USDT'
    interval = '1h'
    current_time = start_time
    data_rows = []
    while current_time < end_time:
        params = {
            'symbol':symbol.replace('/',''),
            'interval':interval,
            'startTime':current_time,
            'endTime':end_time,
            'limit':1000
        }
        try:
            responce = requests.get(url,params=params,timeout=10)
            rows = responce.json()
            if not rows: break

            data_rows.extend(rows)

            last_ts = rows[-1][0]
            if last_ts > end_time: break
            current_time = last_ts + 1
            print(f"已进度至: {datetime.fromtimestamp(current_time/1000)}")
            time.sleep(0.2)
            
        except requests.exceptions.RequestException as e:
            print(f"request error: {e}")

    df = pl.DataFrame(data=data_rows,schema=['open_time','open','height','low','close','volume','close_time','quote_volume','num_trades','taker_buy_base','taker_buy_quote','ignore'],orient='row')
    df = df.with_columns([
        pl.col('open_time').cast(pl.Int64),
        pl.col('open').cast(pl.Float64),
        pl.col('height').cast(pl.Float64),
        pl.col('low').cast(pl.Float64),
        pl.col('close').cast(pl.Float64),
        pl.col('volume').cast(pl.Float64),
        pl.col('close_time').cast(pl.Int64),
        pl.col('quote_volume').cast(pl.Float64),
        pl.col('num_trades').cast(pl.Int64),
        pl.col('taker_buy_base').cast(pl.Float64),
        pl.col('taker_buy_quote').cast(pl.Float64)
    ]).select(['open_time','open','height','low','close','volume','close_time','quote_volume','num_trades','taker_buy_base','taker_buy_quote'])

    df = df.with_columns([
        pl.from_epoch('open_time',time_unit="ms").dt.date().alias('date')
    ])
    df = df.partition_by('date',include_key=True)

    dir_path = f"data/kline/binance/spot/BTC-USDT/{interval}"
    os.makedirs(dir_path,exist_ok=True)
    print(f"length: {len(df)}")
    for row in df:
        file = f"{dir_path}/{row['date'][0].strftime('%Y%m%d')}.parquet"
        if os.path.exists(file):
            os.remove(file)

        tmp_path = f"{file}.tmp"
        row.drop('date').write_parquet(tmp_path,compression="snappy")
        os.replace(tmp_path,file)
        size_mb = os.path.getsize(file) / (1024 * 1024)
        print(f"✅ Processed file saved: {file} | Size: {size_mb:.2f}MB")


if __name__ == '__main__':
    main()