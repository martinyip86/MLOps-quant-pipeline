from src.analytics.indicators import calc_vamp_expr,calc_ofi_expr
from sklearn.linear_model import Ridge,LinearRegression
from sklearn.preprocessing import StandardScaler
from typing import Dict
import polars as pl
import numpy as np

class AlphaResearch:
    def __init__(self,df:pl.DataFrame):
        self.df = df
        self.best_lag = 20
        self.lags = [20, 30, 50, 100, 150, 200, 300, 500]
        self.weights:Dict = {}
        self.best_metrics:dict = {}

    def compute_features(self,depth=5,window=20):
        self.df = self.df.with_columns([
            calc_vamp_expr(depth=depth),
            calc_ofi_expr(window=window)
        ]).with_columns([
            ((pl.col('vamp') - pl.col('micro_price')) / pl.col('micro_price') * 10000).alias('vamp_bias'),
            (pl.col('net_volume_1s') / (pl.col('trade_count_1s') + 1)).alias('norm_net_vol'),
            (pl.col('price_drift_1s') * 10000).alias('trade_drift_bp'),
            (pl.col('imbalance') * pl.col('net_volume_1s').sign()).alias('imb_trade_corr')
        ])
        return self
    
    def label_data(self):
        fee = 2 * 0.0002 * 10000
        # self.df = self.df.with_columns([
        #     pl.col('mid_price').pct_change().rolling_std(window_size=100).alias('volatility')
        # ])

        for lag in self.lags:
            future_micro_avg = pl.col('micro_price').shift(-lag).rolling_mean(window_size=20)

            self.df = self.df.with_columns([
                ((future_micro_avg / pl.col('micro_price') - 1) * 10000).alias(f"target_{lag}_std_return")
            ])

        self.df = self.df.drop_nulls()

        return self

        
    def select_best_lag(self):
        results = []
        estimated_cost_bp = (0.0002 * 10000) + 0.5
        execution_delay = 2
        cols_to_check = ['vamp_bias','ofi','imbalance','norm_net_vol', 'trade_drift_bp']
        for lag in self.lags:
            target_col = [f"target_{lag}_std_return"]
            valid_data = (
                self.df.select(cols_to_check + target_col)
                .filter(pl.all_horizontal(pl.col("*").is_not_null())) # 剔除空值
                .filter(pl.all_horizontal(pl.col("*").is_finite()))   # 剔除 Inf
            )
            if len(valid_data) > 1000:
                X = valid_data.select(cols_to_check).to_numpy()
                y = valid_data.select(target_col).to_numpy().ravel()
                model = LinearRegression()
                model.fit(X,y)
                preds = model.predict(X)
                ic = np.corrcoef(preds,y)[0,1]

                avg_abs_pred = np.mean(np.abs(preds))

                pnl_series = preds * y
                pnl_mean = pnl_series.mean()
                pnl_std = pnl_series.std()

                sharpe = pnl_mean / pnl_std if pnl_std > 0 else 0

                signal = np.sign(preds)
                turnover = np.mean(np.abs(np.diff(signal)))

                edge_ratio = pnl_mean / estimated_cost_bp

                score = ic * sharpe * np.sqrt(lag) / (turnover + 0.01)
                    
                results.append({
                    'lag': lag,
                    'ic': ic,
                    'edge_bp': pnl_mean,
                    'sharpe': sharpe,
                    'turnover': turnover,
                    'score': score
                })

                print(
                    f"Lag: {lag:4d} | IC: {ic:.4f} | Edge: {pnl_mean:.4f} | "
                    f"Sharpe: {sharpe:.3f} | Score: {score:.5f}"
                )

        df_res = pl.DataFrame(results).sort('score',descending=True)

        df_res = df_res.filter((pl.col("turnover") > 0.01) & (pl.col("turnover") < 0.15))

        best = df_res.row(0,named=True)

        self.best_lag = best['lag']

        print("\n🏆 Best Lag Selection:")
        print(df_res.head(5))
        return self
    
    def train_combined_signal(self,split_radio=0.7):
        selected_features = ["vamp_bias","ofi","imbalance","norm_net_vol","trade_drift_bp"]
        target_col = [f"target_{self.best_lag}_std_return"]

        full_df = self.df.select(selected_features + target_col).drop_nulls()
        full_df = full_df.filter(pl.all_horizontal(pl.col('*').is_finite() & pl.all_horizontal(pl.col("*").is_finite())))

        desc = full_df.select(target_col).describe()
        print(f"📊 Label 统计信息: \n{desc}")

        if len(full_df) < 1000:
            print(f"⚠️ [有效样本不足 ({len(full_df)})，跳过训练")
            return
        
        X = full_df.select(selected_features).to_numpy()
        y = full_df.select(target_col).to_numpy().flatten()
        
        n = len(X)
        split_idx = int(n * split_radio)

        X_train,X_valid = X[:split_idx],X[split_idx:]
        y_train,y_valid = y[:split_idx],y[split_idx:]

        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_valid_scaled = scaler.transform(X_valid)

        model = Ridge(alpha=50.0)
        model.fit(X_train_scaled,y_train)

        pred_train = model.predict(X_train_scaled)
        pred_valid = model.predict(X_valid_scaled)

        train_ic = np.corrcoef(pred_train,y_train)[0,1]
        valid_ic = np.corrcoef(pred_valid,y_valid)[0,1]

        signal = pred_valid
        signal_std = np.std(signal) + 1e-8

        self.weights['scaler_mean'] = scaler.mean_.tolist()
        self.weights['scaler_std'] = scaler.scale_.tolist()
        self.weights['coef'] = model.coef_.tolist()
        self.weights['intercept'] = float(model.intercept_)
        self.weights['best_lag'] = self.best_lag
        self.weights["features"] = selected_features
        self.weights["signal_scale"] = signal_std
        self.weights["train_ic"] = float(train_ic)
        self.weights["valid_ic"] = float(valid_ic)
    
        print(f"✅ Training completed")
        print(f"Train IC: {train_ic:.4f}")
        print(f"Valid IC: {valid_ic:.4f}")
        print(f"Weight decay rate: {(train_ic - valid_ic)/train_ic:.2%}")
        print(f"Signal Std: {signal_std:.4f}")

        return self