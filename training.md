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
    
    def loca_historical_data(self,days:int=10):
        symbol = self.symbol.replace('/','-')
        files_path = f"data/processed/{self.exchange_id}/{self.mkt_type}/{symbol}/orderbook/*.parquet"
        files = sorted(glob.glob(files_path))[-days:]

        if not files:
            print("files aren't exists")
            return None
        
        return pl.scan_parquet(files).select(['timestamp','bid_prices','bid_volumes','ask_prices','ask_volumes','micro_price','imbalance','spread','mid_price','sim_buy_price_avg','buy_impact_bps'])
    
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
        self.ch = ch_manager.connect

        df_today = self._get_today_data(self.exchange_id,self.mkt_type,self.symbol,date_str)
        df_history = self.loca_historical_data()

        if df_history is not None:
            df_final = pl.concat([df_history,df_today])
        else:
            df_final = df_today

        df = df_final.sort('timestamp').collect()

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

        backtest = MarketMakingBacktest()
        result = backtest.find_best_maker_threshold(test_df,research.weights)
        df_res = pl.DataFrame(result)

        optimized_results = df_res.filter((pl.col("avg_pnl_bp") > 1.2) & (pl.col("trade_count") >= 25)).sort('avg_pnl_bp',descending=True)

        if optimized_results.is_empty():
            print("backtest don't have 25 trading")
            optimized_results = df_res.sort('total_pnl',descending=True)

        best_row = optimized_results.head(1)
        target_th = best_row["threshold"][0]
        print(f"🚀 Final recommended params for living trading: Threshold={target_th}, avg pnl: {best_row['avg_pnl_bp'][0]}, trades count: {best_row['trade_count'][0]}, total pnl: {best_row['total_pnl'][0]}")
        # maker_vectorized = MakerVectorized()
        # result = maker_vectorized.find_best_maker_threshold(test_df,research.weights)
        # print(result)
        
        # vectorized = Vectorized()
        # vectorized.find_breakeven_threshold(test_df,research.weights)

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
        results = []
        estimated_cost_bp = (0.0002 * 10000) + 0.5
        execution_delay = 2
        for lag in self.lags:
            target_col = f"target_{lag}_tick"
            valid_data = self.df.select(['vamp_bias','ofi','imbalance',target_col]).drop_nulls()
            if len(valid_data) > 1000:
                X = valid_data.select(['vamp_bias','ofi','imbalance']).to_numpy()
                y = valid_data.select([target_col]).to_numpy().ravel()
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
        self.weights["signal_scale"] = float(signal_std)
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

market_making_backtest.py:
import polars as pl
import numpy as np

class MarketMakingBacktest:
    def __init__(self,max_inventory=10.0):
        self.max_inventory = max_inventory
        self.market_fee = 0.0010
        self.slippage_bp = 3.0

    def backtest(self,df:pl.DataFrame,weights:dict,threshold:float=2.0,gamma=3.0,skew: float = 0.0002):
        coef = weights['coef']
        intercept = weights['intercept']
        lag = weights['best_lag']
        scale = weights['signal_scale']

        df_bt = df.with_columns([
            ((pl.col('vamp_bias') * coef[0] + pl.col('ofi') + coef[1] + pl.col('imbalance') * coef[2] + intercept) / scale).alias('z_score')
        ])

        df_bt = df_bt.with_columns([
            (pl.col('z_score') > threshold).alias('quote_buy'),
            (pl.col('z_score') < -threshold).alias('quote_sell')
        ])

        # df_bt = df_bt.with_columns([
        #     pl.col('mid_price').diff().alias('price_change')
        # ]).with_columns([
        #     pl.when((pl.col('side_buy') == 1) & (pl.col('price_change') < 0)).then(1).otherwise(0).alias('filled_buy'),
        #     pl.when((pl.col('side_sell') == -1) & (pl.col('price_change') > 0)).then(1).otherwise(0).alias('filled_sell')
        # ])
        spread = 2 / 10000

        df_bt = df_bt.with_columns([
            pl.col('mid_price').shift(-1).alias('next_mid')
        ])

        z_scores = df_bt['z_score'].to_numpy()
        mid = df_bt['mid_price'].to_numpy()
        next_mid = df_bt['next_mid'].to_numpy()

        quote_buy = df_bt['quote_buy'].to_numpy()
        quote_sell = df_bt['quote_sell'].to_numpy()
        
        n = len(df_bt)

        inventory = np.zeros(n)
        current_inv = 0.0
        pnl = np.zeros(n)

        slippage = 3 / 10000

        for i in range(n - 1):
            inv_ratio = current_inv / self.max_inventory
            
            bid = mid[i] - spread / 2 - skew * inv_ratio
            ask = mid[i] + spread / 2 - skew * inv_ratio

            filled_buy = (next_mid[i] <= bid)
            filled_sell = (next_mid[i] >= ask)
            trade = False
            trade_pnl = 0.0
            if quote_buy[i] and filled_buy and current_inv < self.max_inventory:
                current_inv += 1
                trade_pnl -= spread / 2
                trade = True

            if quote_sell[i] and filled_sell and current_inv > -self.max_inventory:
                current_inv -= 1
                trade_pnl += spread / 2
                trade = True

            mtm = current_inv * (mid[i+1] - mid[i])

            inventory[i] = current_inv
            pnl[i] = trade_pnl + mtm - slippage * trade

        df_bt =df_bt.with_columns([
            pl.Series(name='inventory',values=inventory),
            pl.Series(name='step_pnl',values=pnl)
        ])

        # print(f"--- maker backtest (Th: {threshold}, MaxInv: {self.max_inventory}, Gamma: {gamma}) ---")
        # print(f"Total pnl: {df_bt['step_pnl'].sum():.4%}")
        # print(f"Max inventory: {df_bt['inventory'].max()} | Min inventory: {df_bt['inventory'].min()}")
        # print(f"Avg inventory: {df_bt['inventory'].abs().mean():.2f}")
        
        return df_bt
    
    def find_best_maker_threshold(self, df: pl.DataFrame,weights:dict):
        results = []
        thresholds = np.arange(0.5, 2.5, 0.25)
        for th in thresholds:
            bt = self.backtest(df,weights,th)

            pnl = bt['step_pnl'].sum()
            trades = bt.filter(pl.col('step_pnl') != 0).height
            avg_pnl = pnl / trades if trades > 0 else 0

            results.append({
                "threshold": th,
                "total_pnl": pnl,
                "trade_count": trades,
                "avg_pnl_bp": avg_pnl * 10000
            })

        return results

END




