from src.analytics.indicators import calc_vamp_expr,calc_ofi_expr
from sklearn.linear_model import Ridge,HuberRegressor
from sklearn.preprocessing import StandardScaler
from lightgbm import LGBMRegressor,early_stopping,log_evaluation
from scipy.stats import pearsonr
import polars as pl
import numpy as np
import sys

class AlphaResearch:
    def __init__(self,df:pl.DataFrame):
        self.df = df
        self.best_lag = 20
        self.lags = [20,40,60,80,100]
        self.weights:dict = {}
        self.best_metrics:dict = {}

    def compute_features(self,depth=5,window=20):
        self.df = self.df.with_columns([
            calc_vamp_expr(depth=depth),
            calc_ofi_expr()
        ]).with_columns([
            ((pl.col('vamp') - pl.col('micro_price')) / pl.col('micro_price') * 10000).alias('vamp_bias'),
            (pl.col('net_volume_1s') / (pl.col('trade_count_1s') + 1)).alias('norm_net_vol'),
            (pl.col('price_drift_1s').shift(1) * 10000).alias('trade_drift_bp'),
            (pl.col('imbalance') * pl.col('net_volume_1s').sign()).alias('imb_trade_corr')
        ]).with_columns([
            pl.col('mid_price').rolling_std(window_size=100).fill_null(strategy='forward').alias('fast_vol'),
            pl.col('mid_price').rolling_std(window_size=2000).fill_null(strategy='forward').alias('slow_vol')
        ]).with_columns([
            ((pl.col('bid_prices').list.slice(0,20) * pl.col('bid_volumes').list.slice(0,20)).list.sum() / (pl.col('bid_volumes').list.slice(0,20).list.sum() + 1e-8)).alias('sim_buy_avg')
        ]).with_columns([
            ((pl.col('sim_buy_avg') / pl.col('mid_price') - 1) * 10000).alias('buy_impact_bps')
        ])
        self.df = self.df.drop_nans()
        for depth in [10]:
            self.df = self.df.with_columns([
                ((pl.col('bid_volumes').list.slice(0,depth).list.sum() - pl.col('ask_volumes').list.slice(0,depth).list.sum()) / (pl.col('bid_volumes').list.slice(0,depth).list.sum() + pl.col('ask_volumes').list.slice(0,depth).list.sum() + 1e-8)).alias(f"imbalance_d{depth}")
            ])

        for window in [100]:
            self.df = self.df.with_columns([
                pl.col('ofi').rolling_mean(window_size=window).alias(f'ofi_mean{window}')
            ])

        self.df = self.df.with_columns([
            pl.col('vamp_bias').rolling_std(50).alias('vamp_bias_vol')
        ])

        self.df = self.df.with_columns([
            ((pl.col('micro_price') - pl.col('mid_price')) / pl.col('mid_price') * 10000).alias('micro_vs_mid_bp'),
            (((pl.col('micro_price') - pl.col('mid_price')).rolling_mean(100)) / pl.col('mid_price') * 10000).alias('micro_reversion')
        ])

        self.df = self.df.with_columns([
            (pl.col('bid_volumes').list.slice(0,5).list.sum() / pl.col('bid_volumes').list.slice(0,20).list.sum()).alias('bid_depth_attenuation_ratio'),
            (pl.col('ask_volumes').list.slice(0,5).list.sum() / pl.col('ask_volumes').list.slice(0,20).list.sum()).alias('ask_depth_attenuation_ratio')
        ])

        self.df = self.df.with_columns([
            (pl.col('spread') / pl.col('micro_price') * 10000).alias('spread_bp')
        ])
        return self
    
    def label_data(self):
        for lag in self.lags:
            future_micro_avg = pl.col('micro_price').rolling_mean(window_size=20).shift(-lag)
            future_micro_ewm = pl.col('micro_price').ewm_mean(span=lag//2,adjust=False).shift(-lag)
            future_micro_max = pl.col('micro_price').shift(-lag).rolling_max(window_size=lag)
            future_micro_min = pl.col('micro_price').shift(-lag).rolling_min(window_size=lag)

            current_price = pl.col('micro_price')

            max_ret = (future_micro_max / current_price - 1) * 10000
            min_ret = (future_micro_min / current_price - 1) * 10000

            self.df = self.df.with_columns([
                # ((future_micro_avg / pl.col('micro_price') - 1) * 10000).alias(f"target_{lag}")
                # ((future_micro_ewm / pl.col('micro_price') - 1) * 10000).alias(f'target_{lag}')
                pl.when((max_ret > 2.0) & (min_ret > -1.0)).then(max_ret).otherwise(0.0).alias(f"target_{lag}")
            ])

        self.df = self.df.drop_nulls()

        return self

    def select_best_lag_for_ticker(self):
        results = []
        estimated_cost_bp = 0.5
        cols_to_check = ['imbalance','imbalance_d10','ofi_mean100','vamp_bias_vol','micro_vs_mid_bp','micro_reversion','spread_bp',"htf_trend_ratio", "dist_to_support", "is_sweep"]
            
        
    def select_best_lag(self):
        results = []
        # estimated_cost_bp = (0.0002 * 10000) + 0.5
        estimated_cost_bp = 0
        # cols_to_check = ['vamp_bias','imbalance','imbalance_d5','imbalance_d10','imbalance_d20','ofi_mean10','ofi_mean20','ofi_mean30','ofi_mean60','vamp_bias_mom','vamp_bias_vol','micro_vs_mid_bp','micro_reversion','bid_depth_attenuation_ratio','ask_depth_attenuation_ratio','spread_bp','spread_bp_change_pct','spread_bp_ma5_diff','buy_impact_bps',"htf_trend_ratio", "dist_to_support", "is_sweep"]
        cols_to_check = ['imbalance','imbalance_d10','ofi_mean100','vamp_bias_vol','micro_vs_mid_bp','micro_reversion','spread_bp',"htf_trend_ratio", "dist_to_support", "is_sweep"]
        for lag in self.lags:
            target_col = [f"target_{lag}"]
            data = (
                self.df.select(cols_to_check + target_col)
                .filter(pl.all_horizontal(pl.col("*").is_not_null())) # 剔除空值
                .filter(pl.all_horizontal(pl.col("*").is_finite()))   # 剔除 Inf
            )
            print(f"len: {len(data)}")
            if len(data) > 1000:
                n = len(data)
                idx = int(n * 0.7)
                X = data.select(cols_to_check).to_pandas()
                y = data.select(target_col).to_pandas().values.ravel()

                X_train,X_valid = X.iloc[:idx],X.iloc[idx:]
                y_train,y_valid = y[:idx],y[idx:]

                scaler = StandardScaler()
                X_train_scaler = scaler.fit_transform(X_train)
                X_valid_scaler = scaler.transform(X_valid)

                huber_model = HuberRegressor(max_iter=2000)
                huber_model.fit(X_train_scaler,y_train)
                huber_preds = huber_model.predict(X_valid_scaler)

                lgb_model = LGBMRegressor(
                    n_estimators=300,
                    max_depth=5,
                    num_leaves=31,
                    learning_rate=0.03,
                    min_child_samples=20,
                    min_split_gain=0.0,
                    min_child_weight=0.001,
                    reg_alpha=0.1,
                    reg_lambda=0.1,
                    subsample=0.8,
                    colsample_bytree=0.8,
                    random_state=42,
                    verbose=-1,
                    importance_type="gain"
                )
                lgb_model.fit(
                    X_train,
                    y_train,
                    eval_set=[(X_valid,y_valid)],
                    callbacks=[early_stopping(50), log_evaluation(0)]
                )
                lgb_preds = lgb_model.predict(X_valid)

                feat_imp = pl.DataFrame({
                    'feature':cols_to_check,
                    'importance':lgb_model.feature_importances_
                })

                feat_imp = feat_imp.sort('importance',descending=True)
                print("特征重要性")
                for row in feat_imp.rows():
                    print(f"{row[0]}:{row[1]}")

                final_preds = (huber_preds * 0.4) + (lgb_preds * 0.6)

                ic,p_value = pearsonr(final_preds,y_valid)

                signal = np.sign(final_preds)
                turnover = np.mean(np.abs(np.diff(signal))) / 2

                pnl_series = signal * y_valid
                pnl_mean = pnl_series.mean()
                pnl_std = pnl_series.std()

                sharpe = (pnl_mean - estimated_cost_bp) / pnl_std if pnl_std > 0 else 0

                if p_value < 0.01:
                    score = ic * sharpe * np.sqrt(lag) / (turnover + 0.01)
                else:
                    score = 0
                    
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
                    f"Sharpe: {sharpe:.3f} | Score: {score:.5f} | P-Value: {p_value:.4f}"
                )

        df_res = pl.DataFrame(results).sort('score',descending=True)

        df_res = df_res.filter((pl.col("turnover") > 0.01) & (pl.col("turnover") < 0.15))
        if not df_res.is_empty():
            best = df_res.row(0,named=True)

            self.best_lag = best['lag']

            print("\n🏆 Best Lag Selection:")
            print(df_res.head(5))
        return self
    
    def train_combined_signal(self,split_radio=0.7):
        selected_features = ["vamp_bias","ofi","imbalance","htf_trend_ratio", "dist_to_support", "is_sweep"]
        target_col = [f"target_{self.best_lag}"]

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