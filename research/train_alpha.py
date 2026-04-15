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