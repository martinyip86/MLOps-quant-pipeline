train_alpha.py:
from src.storage.clickhouse.client import ch_manager
from src.utils.weight_manager import WeightManager
from research.factor_analysis import AlphaResearch
from research.backtest.vectorized import Vectorized
from research.backtest.maker_vectorized import MakerVectorized
from research.backtest.market_making_backtest import MarketMakingBacktest
from datetime import datetime,timedelta,timezone
import polars as pl
import numpy as np
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
                (bid_prices[1] * ask_volumes[1] + ask_prices[1] * bid_volumes[1]) / nullIf(bid_volumes[1] + ask_volumes[1],0) AS micro_price,
                (bid_volumes[1] - ask_volumes[1]) / nullIf(bid_volumes[1] + ask_volumes[1],0) AS imbalance,
                ask_prices[1] - bid_prices[1] AS spread,
                (bid_prices[1] + ask_prices[1]) / 2 as mid_price,
                (
                    arraySum(
                        arrayMap(
                            (p,v) -> p * v,
                            arraySlice(ask_prices,1,20),
                            arraySlice(ask_volumes,1,20)
                        )
                    ) /
                    nullIf(arraySum(arraySlice(ask_volumes,1,20)),0)
                ) AS sim_buy_price_avg,
                ((sim_buy_price_avg / mid_price) - 1) * 10000 AS buy_impact_bps
            FROM market_data.orderbook_{mkt_type}
            WHERE exchange_id='{exchange_id}'
                AND symbol='{symbol}'
                AND toDate(fromUnixTimestamp64Milli(timestamp)) = '{date_str}'
            ORDER BY timestamp ASC
        """

        arrow_table = self.ch.query_arrow(sql)
        return pl.from_arrow(arrow_table).lazy()
    
    def loca_historical_data(self,days:int=5):
        symbol = self.symbol.replace('/','-')
        files_path = f"data/processed/{self.exchange_id}/{self.mkt_type}/{symbol}/orderbook/*.parquet"
        files = sorted(glob.glob(files_path))[-days:]

        if not files:
            print("files aren't exists")
            return None
        
        return pl.scan_parquet(files).select(['timestamp','bid_prices','bid_volumes','ask_prices','ask_volumes','micro_price','imbalance','spread','mid_price','sim_buy_price_avg','buy_impact_bps'])
    
    def _get_today_trades(self,exchange_id:str,mkt_type:str,symbol:str,date_str:str):
        sql = f"""
            SELECT
                price,
                amount,
                side,
                timestamp
            FROM market_data.trades_{mkt_type}
            WHERE exchange_id='{exchange_id}'
                AND symbol='{symbol}'
                AND toDate(fromUnixTimestamp64Milli(timestamp)) = '{date_str}'
            ORDER BY timestamp ASC
        """
        arrow_table = self.ch.query_arrow(sql)
        return pl.from_arrow(arrow_table).lazy()
    
    def loca_historical_trades(self,days:int=5):
        symbol = self.symbol.replace('/','-')
        file_path = f"data/processed/{self.exchange_id}/{self.mkt_type}/{symbol}/trades/*.parquet"
        files = sorted(glob.glob(file_path))[-days:]

        if not files:
            print("files aren't exists")
            return None
        
        return pl.scan_parquet(files).select(['price','amount','side','timestamp'])
    
    def _save_weight(self,weights):
        model_weight = WeightManager()
        old_weight = model_weight.load_weight(self.exchange_id,self.mkt_type,self.symbol,'orderbook')

        if old_weight is not None:
            alpha = 0.2
            old_coef = np.array(old_weight['coef'])
            new_coef = np.array(weights['coef'])
            weights['coef'] = (
                old_coef * (1 - alpha) + new_coef * alpha
            ).tolist()

            weights['intercept'] = old_weight['intercept'] * (1 - alpha) + weights['intercept'] * alpha
            weights['signal_scale'] = old_weight['signal_scale'] * (1 - alpha) + weights['signal_scale'] * alpha

        path = model_weight.save_weight(weights,self.exchange_id,self.mkt_type,self.symbol,'orderbook')
        print(f"Traning completed,model save to {path}")

    def main(self):
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        self.ch = ch_manager.connect()

        df_final_ob = self.loca_historical_data()

        df_final_ob = df_final_ob.sort('timestamp')

        df_final_trades = self.loca_historical_trades()

        df_final_trades = df_final_trades.sort('timestamp').rolling(index_column='timestamp',period="1000i").agg([
            pl.col('amount').filter(pl.col('side') == 'buy').sum().fill_null(0).alias('buy_vol_1s'),
            pl.col('amount').filter(pl.col('side') == 'sell').sum().fill_null(0).alias('sell_vol_1s'),
            (pl.col('amount') * pl.when(pl.col('side') == 'buy').then(1).otherwise(-1)).sum().alias('net_volume_1s'),
            pl.len().alias('trade_count_1s'),
            ((pl.col('price').mean() / pl.col('price').last()) - 1).alias('price_drift_1s')
        ])

        df = df_final_ob.join_asof(df_final_trades,on='timestamp',strategy="backward").collect()

        n = len(df)
        idx = int(n * 0.7)
        train_df = df[:idx]
        test_df = df[idx:]

        research = AlphaResearch(train_df)
        research.compute_features().label_data().select_best_lag().train_combined_signal()

        self._save_weight(research.weights)

        test = AlphaResearch(test_df)
        test.compute_features().label_data()
        test_df = test.df.drop_nulls()

        backtest = MarketMakingBacktest(max_inventory=5.0)

        results = []
        for th in np.arange(1.0,2.5,0.5):
            for skew in [0.2,0.5,1.0]:
                res_df = backtest.backtest(test_df,research.weights,th,skew)

                total_pnl = res_df['step_pnl'].sum()
                trade_count = res_df.filter(pl.col('trade_side') != 0).height

                avg_mid = res_df['mid_price'].mean()
                avg_pnl_bp = (total_pnl / (avg_mid * trade_count)) * 10000

                win_rate = (res_df['step_pnl'] > 0).mean()

                # 计算夏普比率 (基于 step_pnl 的波动)
                daily_std = res_df['step_pnl'].std()
                sharpe = (res_df['step_pnl'].mean() / daily_std * np.sqrt(len(res_df))) if daily_std > 0 else 0

                cum_pnl = res_df['step_pnl'].cum_sum()
                max_pnl = cum_pnl.cum_max()
                drawdown = max_pnl - cum_pnl
                max_dd = drawdown.max()

                results.append({
                    "threshold": th,
                    "skew": skew,
                    "total_pnl": total_pnl,
                    "trade_count": trade_count,
                    "avg_pnl_bp": avg_pnl_bp,
                    "sharpe": sharpe,
                    "max_drawdown": max_dd,
                    "win_rate": win_rate,
                    "avg_abs_inventory": res_df["inventory"].abs().mean()
                })

        # 3. 打印最优结果
        best_res = sorted(results, key=lambda x: x['total_pnl'], reverse=True)[0]
        print(f"Final Selection: {best_res}")

if __name__=='__main__':
    trainAlpha = TrainAlpha()
    trainAlpha.main()
END

factor_analysis.py:
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

END

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

    return (db - da).rolling_mean(window_size=window).alias('ofi')

END

import polars as pl
import numpy as np

class MarketMakingBacktest:
    def __init__(self,max_inventory=10.0,tick_size=0.1):
        self.max_inventory = max_inventory
        self.tick_size = tick_size
        self.market_fee_bp = 0.1
        self.slippage_bp = 0.2

    def backtest(self,df:pl.DataFrame,weights:dict,threshold:float=1.5,skew_factor: float = 0.5):
        coef = np.array(weights['coef'])
        means = np.array(weights['scaler_mean'])
        stds = np.array(weights['scaler_std'])
        intercept = weights['intercept']
        scale = weights['signal_scale']
        
        vamp_bias = df['vamp_bias'].to_numpy()
        ofi = df['ofi'].to_numpy()
        imbalance = df['imbalance'].to_numpy()

        vamp_scaled = (vamp_bias - means[0]) / stds[0]
        ofi_scaled = (ofi - means[1]) / stds[1]
        imb_scaled = (imbalance - means[2]) / stds[2]

        raw_pred = (vamp_scaled * coef[0] + ofi_scaled * coef[1] + imb_scaled * coef[2] + intercept)

        z_score = raw_pred / scale
        mid = df['mid_price'].to_numpy()
        spread = df['spread'].to_numpy()
        
        n = len(df)

        inventory = np.zeros(n)
        pnl = np.zeros(n)
        current_inv = 0.0
        # 总成本系数 = (手续费 + 滑点) / 10000
        cost_ratio = (self.market_fee_bp + self.slippage_bp) / 10000

        trades_side = np.zeros(n) # 1 for buy, -1 for sell

        for i in range(n - 1):
            # A. 计算报价 (Base Spread + Inventory Skew)
            # 仓位越多，越倾向于卖出：降低 Ask 吸引成交，降低 Bid 防止成交
            inv_risk = current_inv / self.max_inventory
            
            bid_price = mid[i] - spread[i] / 2 - self.market_fee_bp - (inv_risk * skew_factor * self.tick_size)
            ask_price = mid[i] + spread[i] / 2 + self.market_fee_bp - (inv_risk * skew_factor * self.tick_size)

            can_buy = (z_score[i] > threshold) and (current_inv < self.max_inventory)
            can_sell = (z_score[i] < -threshold) and (current_inv > -self.max_inventory)
            filled_buy = False
            filled_sell = False

            if can_buy and mid[i+1] <= bid_price:
                filled_buy = True

            if can_sell and mid[i+1] >= ask_price:
                filled_sell = True

            trade_count = 0
            step_realized_pnl = 0

            if filled_buy:
                current_inv += 1
                step_realized_pnl -= bid_price * (1 + cost_ratio)
                trades_side[i] = 1
                trade_count += 1

            if filled_sell:
                current_inv -= 1
                step_realized_pnl += ask_price * (1 - cost_ratio)
                trades_side[i] = -1
                trade_count += 1

            # E. 计算盯市收益 (Mark-to-Market PnL)
            inventory[i+1] = current_inv
            # 总收益 = 已实现收益 + 仓位价值变动
            mtm = current_inv * (mid[i+1] - mid[i])
            pnl[i+1] = step_realized_pnl + mtm

        df_bt = df.with_columns([
            pl.Series("z_score", z_score),
            pl.Series("inventory", inventory),
            pl.Series("step_pnl", pnl),
            pl.Series("trade_side", trades_side)
        ])
        
        return df_bt
    

END




