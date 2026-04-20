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