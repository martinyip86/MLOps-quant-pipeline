from src.storage.clickhouse.client import ch_manager
from datetime import datetime,timezone
from sklearn.linear_model import LinearRegression
import polars as pl
import numpy as np
import os
import glob

class TrainAlphaExecutor:
    def __init__(self,exchange_id:str='binance',mkt_type:str='spot',symbol:str='BTC/USDT'):
        self.exchange_id = exchange_id
        self.mkt_type = mkt_type
        self.symbol = symbol
        self.ch = ch_manager.connect
        self.model = None

    def _get_today_data(self,date_str:str=None):
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        sql = f"""
            SELECT
                timestamp,
                bid_prices,
                bid_volumes,
                ask_prices,
                ask_volumes,
                ((bid_prices[1] + ask_prices[1]) / 2) AS mid_price,
                ((bid_volumes[1] - ask_volumes[1]) / nullIf((bid_volumes[1] + ask_volumes[1]),0)) AS imbalance
            FROM
                market_data.orderbook_spot
            WHERE exchange_id='{self.exchange_id}'
                AND mkt_type='{self.mkt_type}'
                AND symbol='{self.symbol}'
                AND toDate(fromUnixTimestamp64Milli(timestamp))='{date_str}'
            ORDER BY timestamp ASC
        """
        arrow_table = self.ch.query_arrow(sql)
        return pl.from_arrow(arrow_table).lazy()
    
    def _get_historical_data(self,days:int=10):
        path = os.path.join(
            'data/processed',
            self.exchange_id,
            self.mkt_type,
            self.symbol.replace('/','-'),
            'orderbook/*.parquet'
        )

        files = sorted(glob.glob(path))[-days:]

        if not files:
            print("files are not exists")
            return None
        
        return pl.scan_parquet(files).select('timestamp','bid_prices','bid_volumes','ask_prices','ask_volumes','mid_price','imbalance')
    
    def compute_features(self,df:pl.DataFrame,depth=5,window=20):
        vamp_bid = (pl.col('bid_prices').list.slice(0,depth) * pl.col('bid_volumes').list.slice(0,depth)).list.sum() / pl.col('bid_volumes').list.slice(0,depth).list.sum()
        vamp_ask = (pl.col('ask_prices').list.slice(0,depth) * pl.col('ask_volumes').list.slice(0,depth)).list.sum() / pl.col('ask_volumes').list.slice(0,depth).list.sum()

        b_p = pl.col('bid_prices').list.get(0)
        b_v = pl.col('bid_volumes').list.get(0)
        a_p = pl.col('ask_prices').list.get(0)
        a_v = pl.col('ask_volumes').list.get(0)

        db = pl.when(b_p > b_p.shift(1)).then(b_v).when(b_p == b_p.shift(1)).then(b_v - b_v.shift(1)).otherwise(-b_v.shift(1))
        da = pl.when(a_p > a_p.shift(1)).then(a_v).when(a_p == a_p.shift(1)).then(a_v - a_v.shift(1)).otherwise(-a_v.shift(1))

        df = df.with_columns([
            ((vamp_bid + vamp_ask) / 2).alias('vamp'),
            (db - da).rolling_mean(window).alias('ofi')
        ]).with_columns([
            ((pl.col('vamp') - pl.col('mid_price')) / pl.col('mid_price') * 10000).alias('vamp_bias')
        ])
        return df
    
    def label_data(self,df:pl.DataFrame,lag=10):
        df = df.with_columns([
            pl.col('mid_price').shift(-lag).alias('future_mid_price')
        ]).with_columns([
            ((pl.col('future_mid_price') - pl.col('mid_price')) / pl.col('mid_price')).alias('future_return')
        ])
        return df
    
    def train_combined_signal(self,df:pl.DataFrame,split_radio=0.7):
        seleted_features = ["vamp_bias","ofi","imbalance"]
        target_col = ["future_return"]

        full_df = df.select(seleted_features + target_col).drop_nulls()
        full_df = full_df.filter(pl.all_horizontal(pl.col('*').is_finite()))

        X = full_df.select(seleted_features).to_numpy()
        y = full_df.select(target_col).to_numpy().flatten()

        n = len(X)
        idx = int(n * split_radio)

        X_train,X_valid = X[:idx],X[idx:]
        y_train,y_valid = y[:idx],y[idx:]

        model = LinearRegression()

        model.fit(X_train,y_train)

        y_train_pred = model.predict(X_train)
        y_valid_pred = model.predict(X_valid)

        train_ic = np.corrcoef(y_train_pred,y_train)[0,1]
        valid_ic = np.corrcoef(y_valid_pred,y_valid)[0,1]

        print(f"coef: {model.coef_}")
        print(f"intercept: {model.intercept_}")
        print(f"train_ic: {train_ic}")
        print(f"valid_ic: {valid_ic}")

        print(y_valid_pred)
        signal = np.sign(y_valid_pred)
        print(signal)
        pnl = signal * y_valid
        print(pnl)

        cum_pnl = np.cumsum(pnl)
        win_ratio = np.mean(pnl > 0)

        print(f"最终PnL: {cum_pnl[-1]}")
        print(f"胜率: {win_ratio:.4f}")
    
    def main(self):
        today_data = self._get_today_data()
        historical_data = self._get_historical_data()

        if historical_data is not None:
            df_final = pl.concat([historical_data,today_data])
        else:
            df_final = today_data

        df = df_final.sort('timestamp').collect()
        df = self.compute_features(df)
        df = self.label_data(df)
        df = self.train_combined_signal(df)


if __name__ == '__main__':
    obj = TrainAlphaExecutor()
    obj.main()