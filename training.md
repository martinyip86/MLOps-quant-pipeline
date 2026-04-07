train_alpha.py:
from src.storage.clickhouse.client import ch_manager
from src.utils.weight_manager import WeightManager
from research.factor_analysis import AlphaResearch
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
    
    def _save_weight(self,weights):
        model_weight = WeightManager()
        old_weight = model_weight.load_weight(self.exchange_id,self.mkt_type,self.symbol,'orderbook')

        final_dict = {}

        for side in ["long","short"]:
            final_dict[side] = {
                **weights[side]
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

        research = AlphaResearch(df)
        research.compute_features().label_data().train_combined_signal()

        self._save_weight(research.weights)


if __name__=='__main__':
    trainAlpha = TrainAlpha()
    trainAlpha.main()

factor_analysis.py:
from src.analytics.indicators import calc_vamp_expr,calc_ofi_expr
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from typing import Dict
import polars as pl
import numpy as np

class AlphaResearch:
    def __init__(self,df:pl.DataFrame):
        self.df = df
        self.best_lag_long = 20
        self.best_lag_short = 20
        self.weights:Dict = {
            "long": {
                "w_vamp":0.0,
                "w_ofi":0.0,
                "intercept":0.0,
                "best_lag":0,
                "threshold":0.0,
                "signal_scale":0.0,
                "holding_period":0.0,
                "train_ic":0.0,
                "valid_ic":0.0,
            },
            "short": {
                "w_vamp":0.0,
                "w_ofi":0.0,
                "intercept":0.0,
                "best_lag":0,
                "threshold":0.0,
                "signal_scale":0.0,
                "holding_period":0.0,
                "train_ic":0.0,
                "valid_ic":0.0,
            },
        }
        self.scaler = StandardScaler()

    def compute_features(self,depth=5,window=20):
        self.df = self.df.with_columns([
            calc_vamp_expr(depth=depth),
            calc_ofi_expr(window=window)
        ]).with_columns([
            ((pl.col('vamp') - pl.col('ask_prices').list.get(0)) / pl.col('ask_prices').list.get(0) * 10000).alias('vamp_bias_bp_long'),
            ((pl.col('bid_prices').list.get(0) - pl.col('vamp')) / pl.col('bid_prices').list.get(0) * 10000).alias('vamp_bias_bp_short'),
            (pl.col('bid_volumes').list.get(0) / (pl.col('bid_volumes').list.get(0) + pl.col('ask_volumes').list.get(0) + 1e-8)).alias('imbalance'),
            ((pl.col('bid_prices').list.get(0) * pl.col('ask_volumes').list.get(0) + pl.col('ask_prices').list.get(0) * pl.col('bid_volumes').list.get(0)) / (pl.col('bid_volumes').list.get(0) + pl.col('ask_volumes').list.get(0) + 1e-8)).alias('microprice')
        ])
        return self
    
    def label_data(self,lags=[5,10,20],split_ratio=0.7):
        results_long = {}
        results_short = {}

        fee = 2 * 0.0002 * 10000

        for lag in lags:
            future_micro = self.df['microprice'].shift(-lag)

            self.df = self.df.with_columns([
                ((future_micro - pl.col('microprice')) / pl.col('microprice') * 10000).alias(f"target_{lag}_tick_long"),
                ((pl.col('microprice') - future_micro) / pl.col('microprice') * 10000).alias(f"target_{lag}_tick_short")
            ])

        n = len(self.df)
        split_idx = int(n * split_ratio)
        train_df = self.df.slice(0,split_idx)

        for lag in lags:
            long_target_col = f"target_{lag}_tick_long"
            short_target_col = f"target_{lag}_tick_short"
            valid_long_data = train_df.select(['vamp_bias_bp_long','factor_ofi_smooth','imbalance',long_target_col]).drop_nulls()
            if len(valid_long_data) > 1000:
                X = valid_long_data.select(['vamp_bias_bp_long','factor_ofi_smooth','imbalance']).to_numpy()
                y = valid_long_data.select([long_target_col]).to_numpy().ravel()
                ic = abs(np.corrcoef(X.mean(axis=1),y)[0,1])
                results_long[lag] = ic

            valid_short_data = train_df.select(['vamp_bias_bp_short','factor_ofi_smooth','imbalance',short_target_col]).drop_nulls()
            if len(valid_short_data) > 1000:
                X = valid_short_data.select(['vamp_bias_bp_short','factor_ofi_smooth','imbalance']).to_numpy()
                y = valid_short_data.select([short_target_col]).to_numpy().ravel()
                ic = abs(np.corrcoef(X.mean(axis=1),y)[0,1])
                results_short[lag] = ic

        self.best_lag_long = max(results_long,key=results_long.get)
        self.best_lag_short = max(results_short,key=results_short.get)
        print(f"🚀 [Long] 最佳窗口: {self.best_lag_long} ticks (IC: {results_long[self.best_lag_long]:.4f})")
        print(f"🚀 [Short] 最佳窗口: {self.best_lag_short} ticks (IC: {results_short[self.best_lag_short]:.4f})")
        return self
    
    def train_combined_signal(self,split_radio=0.7):

        def _train_side(side:str,best_lag):
            target_col = f"target_{best_lag}_tick_{side}"
            feature_vamp = f"vamp_bias_bp_{side}"
            imbalance = "imbalance"
            feature_ofi = "factor_ofi_smooth"

            full_df = self.df.select([feature_vamp,feature_ofi,imbalance,target_col]).drop_nulls()
            full_df = full_df.filter(pl.all_horizontal(pl.col('*').is_finite()))

            if len(full_df) < 1000:
                print(f"⚠️ [{side.upper()}] 有效样本不足 ({len(full_df)})，跳过训练")
                return
            
            X = full_df.select([feature_vamp,feature_ofi,imbalance]).to_numpy()
            y = full_df.select([target_col]).to_numpy().flatten()

            X_scaler = self.scaler.fit_transform(X)
            
            n = len(X)
            split_idx = int(n * split_radio)

            X_train,X_valid = X_scaler[:split_idx],X_scaler[split_idx:]
            y_train,y_valid = y[:split_idx],y[split_idx:]

            model = LinearRegression()
            model.fit(X_train,y_train)

            y_pred_train = model.predict(X_train)
            y_pred_valid = model.predict(X_valid)

            mean_pred = np.mean(y_pred_valid)
            y_pred_valid_centered = y_pred_valid - mean_pred
            y_pred_train_centered = y_pred_train - np.mean(y_pred_train)

            train_ic = np.corrcoef(y_pred_train_centered,y_train)[0,1]
            valid_ic = np.corrcoef(y_pred_valid_centered,y_valid)[0,1]
            
            df_tmp = pl.DataFrame({
                "signal": y_pred_valid_centered,
                "ret": y_valid
            })
            threshold_candidates = [0.75, 0.80, 0.85, 0.90, 0.95]
            quantiles = df_tmp["signal"].quantile([0.6, 0.7, 0.8, 0.9])

            best_threshold = 0.0
            best_mean_ret = -999.0

            best_count = 0

            for q in threshold_candidates:
                q_value = df_tmp["signal"].quantile(q)
                if q_value is None:
                    continue
                    
                selected = df_tmp.filter(pl.col("signal") > q_value)
                count = len(selected)
                if count < 200:        # 至少需要200个样本
                    continue
                    
                mean_ret = float(selected["ret"].mean())
                
                if mean_ret > best_mean_ret:
                    best_mean_ret = mean_ret
                    best_threshold = float(q_value)
                    best_count = count

            signal_std = float(np.std(y_pred_valid_centered))

            coef = model.coef_
            self.weights[side]['w_vamp'] = float(coef[0])
            self.weights[side]['w_ofi'] = float(coef[1])
            self.weights[side]['intercept'] = float(model.intercept_)
            self.weights[side]['best_lag'] = best_lag
            self.weights[side]["threshold"] = best_threshold
            self.weights[side]["signal_scale"] = float(np.std(y_pred_valid_centered))
            self.weights[side]["holding_period"] = int(best_lag)
            self.weights[side]["train_ic"] = float(train_ic)
            self.weights[side]["valid_ic"] = float(valid_ic)
            

            print(f"--- [{side.upper()} Side (Lag: {best_lag})] ---")
            print(f"Train IC: {train_ic:.4f} | Valid IC: {valid_ic:.4f}")
            print(f"权重衰减率: {(train_ic - valid_ic)/train_ic:.2%}")

            print(f"✅ [{side.upper()}] 训练完成 | "
                  f"Train IC: {train_ic:.4f} | Valid IC: {valid_ic:.4f} | "
                  f"Threshold: {best_threshold:.4f} (q={best_threshold:.2f}) | "
                  f"Selected Samples: {best_count} | Mean Ret: {best_mean_ret:.4f} bp | "
                  f"Signal Std: {signal_std:.4f}")

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

