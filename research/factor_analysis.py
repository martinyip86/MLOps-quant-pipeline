from src.analytics.indicators import calc_vamp_expr,calc_ofi_expr
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from typing import Dict
import polars as pl
import numpy as np

class AlphaResearch:
    def __init__(self,df:pl.DataFrame):
        self.df = df
        self.best_lag = 20
        self.lags = [1,10,20,30,50,100,150,200]
        self.weights:Dict = {}
        self.scaler = StandardScaler()

    def compute_features(self,depth=5,window=20):
        self.df = self.df.with_columns([
            calc_vamp_expr(depth=depth),
            calc_ofi_expr(window=window)
        ]).with_columns([
            ((pl.col('vamp') - pl.col('micro_price')) / pl.col('micro_price') * 10000).alias('vamp_bias')
        ])
        return self
    
    def label_data(self):
        fee = 2 * 0.0002 * 10000

        for lag in self.lags:
            future_micro = pl.col('micro_price').shift(-lag)

            self.df = self.df.with_columns([
                (((future_micro - pl.col('micro_price')) / pl.col('micro_price')) * 10000).alias(f"target_{lag}_tick")
            ])

        return self

        
    def select_best_lag(self):
        best_ic = -999
        for lag in self.lags:
            target_col = f"target_{lag}_tick"
            valid_data = self.df.select(['vamp_bias','ofi','imbalance',target_col]).drop_nulls()
            if len(valid_data) > 1000:
                X = valid_data.select(['vamp_bias','ofi','imbalance']).to_numpy()
                y = valid_data.select([target_col]).to_numpy().ravel()
                model = LinearRegression()
                model.fit(X,y)
                pred = model.predict(X)
                ic = np.corrcoef(pred,y)[0,1]
                print(f"lag: {lag}|IC: {ic}")
                if abs(ic) > best_ic:
                    best_ic = abs(ic)
                    self.best_lag = lag

        print(f"🚀 最佳窗口: {self.best_lag} ticks (IC: {best_ic:.4f})")
        return self
    
    def train_combined_signal(self,split_radio=0.7):
        selected_features = ["vamp_bias","ofi","imbalance"]
        target_col = [f"target_{self.best_lag}_tick"]

        full_df = self.df.select(selected_features + target_col).drop_nulls()
        full_df = full_df.filter(pl.all_horizontal(pl.col('*').is_finite()))

        if len(full_df) < 1000:
            print(f"⚠️ [有效样本不足 ({len(full_df)})，跳过训练")
            return
        
        X = full_df.select(selected_features).to_numpy()
        y = full_df.select(target_col).to_numpy().flatten()
        
        n = len(X)
        split_idx = int(n * split_radio)

        X_train,X_valid = X[:split_idx],X[split_idx:]
        y_train,y_valid = y[:split_idx],y[split_idx:]

        # self.scaler.fit(X_train)
        # X_train = self.scaler.transform(X_train)
        # X_valid = self.scaler.transform(X_valid)

        model = LinearRegression()
        model.fit(X_train,y_train)

        pred_train = model.predict(X_train)
        pred_valid = model.predict(X_valid)

        train_ic = np.corrcoef(pred_train,y_train)[0,1]
        valid_ic = np.corrcoef(pred_valid,y_valid)[0,1]

        signal = pred_valid
        signal_std = np.std(signal) + 1e-8

        self.weights['coef'] = model.coef_.tolist()
        self.weights['intercept'] = float(model.intercept_)
        self.weights['best_lag'] = self.best_lag
        self.weights["features"] = selected_features
        self.weights["signal_scale"] = float(signal_std)
        self.weights["train_ic"] = float(train_ic)
        self.weights["valid_ic"] = float(valid_ic)
    
        print(f"✅ 训练完成")
        print(f"Train IC: {train_ic:.4f}")
        print(f"Valid IC: {valid_ic:.4f}")
        print(f"权重衰减率: {(train_ic - valid_ic)/train_ic:.2%}")
        print(f"Signal Std: {signal_std:.4f}")

        return self