train_alpha.py:
from src.storage.clickhouse.client import ch_manager
from src.utils.weight_manager import WeightManager
from research.factor_analysis import AlphaResearch
from research.backtest.vectorized import Vectorized
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

        n = len(df)
        idx = int(n * 0.7)
        train_df = df[:idx]
        test_df = df[idx:]

        research = AlphaResearch(train_df)
        research.compute_features().label_data().select_best_lag().train_combined_signal()

        self._save_weight(research.weights)

        test = AlphaResearch(test_df)
        test.compute_features()
        test_df = test.df

        vectorized = Vectorized()
        bt = vectorized.vectorized_backtest(test_df,research.weights)
        vectorized.find_breakeven_threshold(bt)

if __name__=='__main__':
    trainAlpha = TrainAlpha()
    trainAlpha.main()

END

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
        self.best_lag = 20
        self.lags = [10,20,30,50,100,150,200,300,400,500,600,700,800,900,1000,2000]
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

vectorized.py:
import polars as pl
import numpy as np

class Vectorized:
    def __init__(self):
        pass

    def vectorized_backtest(self,df:pl.DataFrame,weights:dict,threshold:float=4.0):
        """
        基于你存储的 coef 和 intercept 进行回测
        """
        # 1. 重构特征 (这一步必须与训练时完全一致)
        # 假设 df 已经包含了指标计算
        
        # 2. 计算组合信号 (Z-Score)
        # 公式: (X * coef + intercept) / signal_scale
        coef = weights['coef']
        intercept = weights['intercept']
        scale = weights['signal_scale']
        df_bt = df.with_columns([
            ((pl.col('vamp_bias') * coef[0] + pl.col('ofi') * coef[1] + pl.col('imbalance') * coef[2] + intercept) / scale).alias('z_score')
        ])

        # 3. 模拟持仓 (核心：必须 shift(1) 避免前瞻偏差)
        df_bt = df_bt.with_columns([
            (pl.when(pl.col('z_score') > threshold).then(1).when(pl.col('z_score') < -threshold).then(-1).otherwise(0)).alias('raw_pos')
        ]).with_columns([
            pl.col('raw_pos').shift(1).fill_null(0).alias('position')
        ])

        # 4. 计算真实收益 (使用 Tick 级的变动)
        # 注意：这里建议使用 mid_price 的百分比变动，而不是 target_lag
        df_bt = df_bt.with_columns([
            (pl.col('position') * (pl.col('mid_price').diff() / pl.col('mid_price').shift(1))).alias('pnl_raw')
        ])

        # 5. 扣除手续费 (每当 position 变化时扣除)
        fee_rate = 0.0002
        df_bt = df_bt.with_columns([
            (pl.col('position').diff().abs() * fee_rate).fill_null(0).alias('cost')
        ]).with_columns([
            (pl.col('pnl_raw') - pl.col('cost') + (pl.col('spread') / 2)).alias('pl_net')
        ])
        print(f"当前模型最大信号强度: {df_bt['z_score'].abs().max()}")
        print(f"累计净收益: {df_bt['pl_net'].sum():.4%}")
        return df_bt
    
    def find_breakeven_threshold(self,df:pl.DataFrame,fee_bps=4.0):
        results = []

        for th in np.arange(4.0,10.0,0.5):
            trades = df.filter(pl.col('z_score').abs() > th)
            if len(trades) == 0: continue

            avg_alpha = trades['pnl_raw'].mean() * 10000
            net_pnl = (avg_alpha - fee_bps) * len(trades)

            print(f"Th: {th:.1f} | 交易次数: {len(trades)} | 单笔Alpha: {avg_alpha:.2f}bp | 净收益: {net_pnl:.2f}")

END




