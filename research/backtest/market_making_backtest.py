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
            
            bid_price = mid[i] - spread[i] / 2 - (inv_risk * skew_factor * self.tick_size)
            ask_price = mid[i] + spread[i] / 2 - (inv_risk * skew_factor * self.tick_size)

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
    