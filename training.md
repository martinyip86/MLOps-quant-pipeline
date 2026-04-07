train_alpha.py:
from src.storage.clickhouse.client import ch_manager
from src.utils.weight_manager import WeightManager
from research.factor_analysis import AlphaRearch
from datetime import datetime,timedelta,timezone
import polars as pl
import glob

class TrainAlpha:
    def __init__(self,exchange_id:str='binance',mkt_type:str='spot',symbol:str='BTC/USDT'):
        self.ch = None
        self.exchange_id = exchange_id
        self.mkt_type = mkt_type
        self.symbol = symbol

    def _get_today_data(self,exchange_id:str,mkt_type:str,symbol:str,date_str:str):
        sql = f"""
            SELECT 
                timestamp,
                bid_prices,
                bid_volumes,
                ask_prices,
                ask_volumes,
                (bid_prices[1] + ask_prices[1]) / 2 as mid_price
            FROM market_data.orderbook_{mkt_type}
            WHERE exchange_id='{exchange_id}'
                AND symbol='{symbol}'
                AND toDate(fromUnixTimestamp64Milli(timestamp)) = '{date_str}'
            ORDER BY timestamp ASC
        """

        arrow_table = self.ch.query_arrow(sql)
        return pl.from_arrow(arrow_table).lazy()
    
    def loca_historical_data(self,days:int=10):
        symbol = self.symbol.replace('/','-')
        files_path = f"data/processed/{self.exchange_id}/{self.mkt_type}/{symbol}/orderbook/*.parquet"
        files = sorted(glob.glob(files_path))[-days:]

        if not files:
            print("files aren't exists")
            return None
        
        return pl.scan_parquet(files).with_columns([
            ((pl.col('bid_prices').list.get(0) + pl.col('ask_prices').list.get(0)) / 2).alias('mid_price')
        ]).select(['timestamp','bid_prices','bid_volumes','ask_prices','ask_volumes','mid_price'])
    
    def _save_weight(self,weights,metrics):
        model_weight = WeightManager()
        old_weight = model_weight.load_weight(self.exchange_id,self.mkt_type,self.symbol,'orderbook')

        final_dict = {}

        for side in ["long","short"]:
            final_dict[side] = {
                **weights[side],
                **metrics[side]
            }

        if old_weight is not None:
            alpha = 0.2
            for side in ["long","short"]:
                for key in ["w_vamp","w_ofi"]:
                    old_value = old_weight[side][key]
                    new_value = final_dict[side][key]
                    final_dict[side][key] = old_value * (1 - alpha) + new_value * alpha

        path = model_weight.save_weight(final_dict,self.exchange_id,self.mkt_type,self.symbol,'orderbook')
        print(f"训练完成，模型已存至 {path}")

    def main(self):
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        self.ch = ch_manager.connect

        df_today = self._get_today_data(self.exchange_id,self.mkt_type,self.symbol,date_str)
        df_history = self.loca_historical_data()

        if df_history is not None:
            df_final = pl.concat([df_history,df_today])
        else:
            df_final = df_today

        df = df_final.sort('timestamp').collect()

        research = AlphaRearch(df)
        research.compute_features().label_data().train_combined_signal()

        self._save_weight(research.weights,research.metrics)


if __name__=='__main__':
    trainAlpha = TrainAlpha()
    trainAlpha.main()

factor_analysis.py:
from src.analytics.indicators import calc_vamp_expr,calc_ofi_expr
from sklearn.linear_model import LinearRegression
import polars as pl
import numpy as np

class AlphaRearch:
    def __init__(self,df:pl.DataFrame):
        self.df = df
        self.best_lag_long = 20
        self.best_lag_short = 20
        self.weights = {
            "long": {"w_vamp":0.0,"w_ofi":0.0,"intercept":0.0},
            "short": {"w_vamp":0.0,"w_ofi":0.0,"intercept":0.0},
        }
        self.metrics = {
            "long": {},
            "short": {}
        }

    def compute_features(self,depth=5,window=20):
        self.df = self.df.with_columns([
            calc_vamp_expr(depth=depth),
            calc_ofi_expr(window=window)
        ]).with_columns([
            ((pl.col('vamp') - pl.col('ask_prices').list.get(0)) / pl.col('ask_prices').list.get(0) * 10000).alias('vamp_bias_bp_long'),
            ((pl.col('bid_prices').list.get(0) - pl.col('vamp')) / pl.col('bid_prices').list.get(0) * 10000).alias('vamp_bias_bp_short')
        ])
        return self
    
    def label_data(self,lags=[5,10,20,50,100],split_radio=0.7):
        results_long = {}
        results_short = {}

        curr_ask = self.df['ask_prices'].list.get(0)
        curr_bid = self.df['bid_prices'].list.get(0)
        fee = 2 * 0.0002 * 10000

        for lag in lags:
            future_ask = curr_ask.shift(-lag)
            future_bid = curr_bid.shift(-lag)

            self.df = self.df.with_columns([
                ((future_bid - curr_ask) / curr_ask * 10000 - fee).alias(f"target_{lag}_tick_long"),
                ((curr_bid - future_ask) / curr_bid * 10000 - fee).alias(f"target_{lag}_tick_short")
            ])

        n = len(self.df)
        split_idx = int(n * split_radio)
        train_df = self.df.slice(0,split_idx)

        for lag in lags:
            long_target_col = f"target_{lag}_tick_long"
            short_target_col = f"target_{lag}_tick_short"
            valid_long_data = train_df.select(['vamp_bias_bp_long',long_target_col]).drop_nulls()
            if len(valid_long_data) > 0:
                long_ic = abs(valid_long_data.select(pl.corr('vamp_bias_bp_long',long_target_col)).item())
                results_long[lag] = long_ic
            else:
                results_long[lag] = 0

            valid_short_data = train_df.select(['vamp_bias_bp_short',short_target_col]).drop_nulls()
            if len(valid_short_data) > 0:
                short_ic = abs(valid_short_data.select(pl.corr('vamp_bias_bp_short',short_target_col)).item())
                results_short[lag] = short_ic
            else:
                results_short[lag] = 0

        self.best_lag_long = max(results_long,key=results_long.get)
        self.best_lag_short = max(results_short,key=results_short.get)
        print(f"🚀 [Long] 最佳窗口: {self.best_lag_long} ticks (IC: {results_long[self.best_lag_long]:.4f})")
        print(f"🚀 [Short] 最佳窗口: {self.best_lag_short} ticks (IC: {results_short[self.best_lag_short]:.4f})")
        return self
    
    def train_combined_signal(self,split_radio=0.7):

        def _train_side(side:str,best_lag):
            target_col = f"target_{best_lag}_tick_{side}"
            feature_vamp = f"vamp_bias_bp_{side}"
            feature_ofi = "factor_ofi_smooth"

            full_df = self.df.select([feature_vamp,feature_ofi,target_col]).drop_nulls()
            full_df = full_df.filter(pl.all_horizontal(pl.col('*').is_finite()))

            if len(full_df) < 1000:
                print(f"⚠️ [{side.upper()}] 有效样本不足 ({len(full_df)})，跳过训练")
                return
            
            n = len(full_df)
            split_idx = int(n * split_radio)

            train_df = full_df.slice(0,split_idx)
            valid_df = full_df.slice(split_idx,n - split_idx)

            X_train = train_df.select([feature_vamp,feature_ofi]).to_numpy()
            y_train = train_df.select([target_col]).to_numpy().flatten()

            model = LinearRegression()
            model.fit(X_train,y_train)

            X_valid = valid_df.select([feature_vamp,feature_ofi]).to_numpy()
            y_valid = valid_df.select([target_col]).to_numpy().flatten()

            y_pred_train = model.predict(X_train)
            y_pred_valid = model.predict(X_valid)

            df_tmp = pl.DataFrame({
                "signal": y_pred_valid,
                "ret": y_valid
            })

            bins = np.linspace(y_pred_valid.min(),y_pred_valid.max(),20)
            df_tmp = df_tmp.with_columns([
                pl.when(pl.col("signal") > 2).then(2)
                .when(pl.col("signal") > 1).then(1)
                .when(pl.col("signal") > 0).then(0)
                .when(pl.col("signal") > -1).then(-1)
                .when(pl.col("signal") > -2).then(-2)
                .otherwise(-3)
                .alias("bucket")
            ])

            bucket_stats = df_tmp.group_by("bucket").agg([
                pl.mean('ret').alias('mean_ret'),
                pl.count().alias('cnt')
            ]).sort('bucket')

            threshold = 0.0
            for row in bucket_stats.iter_rows(named=True):
                if row['mean_ret'] > 0 and row['cnt'] > 50:
                    threshold = bins[int(row["bucket"])]
                    break

            self.metrics[side]["threshold"] = threshold

            scale = np.std(y_pred_valid)
            self.metrics[side]["signal_scale"] = float(scale)
            self.metrics[side]["holding_period"] = int(best_lag)

            valid_ic = np.corrcoef(y_pred_valid.flatten(),y_valid.flatten())[0,1]
            train_ic = np.corrcoef(y_pred_train.flatten(),y_train.flatten())[0,1]

            print(f"--- [{side.upper()} Side (Lag: {best_lag})] ---")
            print(f"Train IC: {train_ic:.4f} | Valid IC: {valid_ic:.4f}")
            print(f"权重衰减率: {(train_ic - valid_ic)/train_ic:.2%}")

            if valid_ic > 0.01:
                X_full = full_df.select([feature_vamp,feature_ofi]).to_numpy()
                y_full = full_df.select([target_col]).to_numpy().flatten()

                model.fit(X_full,y_full)

                coef = model.coef_.flatten()

                intercept = model.intercept_
                if isinstance(intercept, np.ndarray):
                    intercept = intercept.item()
                self.weights[side]['w_vamp'],self.weights[side]['w_ofi'] = coef
                self.weights[side]['intercept'] = intercept
                self.weights[side]['best_lag'] = best_lag
                self.metrics[side]["train_ic"] = float(train_ic)
                self.metrics[side]["valid_ic"] = float(valid_ic)

                print(f"✅ {side.upper()} 权重已保存: {self.weights[side]}·")
            else:
                print(f"❌ {side.upper()} 验证集表现不佳，未更新权重")

        _train_side('long',self.best_lag_long)
        _train_side('short',self.best_lag_short)

        return self

indictors.py:
import polars as pl
import numpy as np

def calc_vamp_expr(depth=5) -> pl.Series:
    vamp_bid = (pl.col('bid_prices').list.slice(0,depth) * pl.col('bid_volumes').list.slice(0,depth)).list.sum() / pl.col('bid_volumes').list.slice(0,depth).list.sum()
    vamp_ask = (pl.col('ask_prices').list.slice(0,depth) * pl.col('ask_volumes').list.slice(0,depth)).list.sum() / pl.col('ask_volumes').list.slice(0,depth).list.sum()

    return ((vamp_bid + vamp_ask) / 2).alias('vamp')

def calc_ofi_expr(window:int=20) -> pl.Series:
    p_b = pl.col('bid_prices').list.get(0)
    v_b = pl.col('bid_volumes').list.get(0)
    p_a = pl.col('ask_prices').list.get(0)
    v_a = pl.col('ask_volumes').list.get(0)

    db = pl.when(p_b > p_b.shift(1)).then(v_b).when(p_b==p_b.shift(1)).then(v_b - v_b.shift(1)).otherwise(-v_b.shift(1))
    da = pl.when(p_a < p_a.shift(1)).then(v_a).when(p_a == p_a.shift(1)).then(v_a - v_a.shift(1)).otherwise(-v_a.shift(1))

    return (db - da).rolling_mean(window_size=window).alias('factor_ofi_smooth')

