import polars as pl
import numpy as np
import logging
from src.utils.logger import setup_logger

class MarketMakingBacktest:
    def __init__(self,max_inventory=10.0,tick_size=0.1):
        self.max_inventory = max_inventory
        self.tick_size = tick_size
        self.market_fee_bp = 0.0 #0.1
        self.slippage_bp = 0.0 #0.2
        self.usdt = 100000
        self.inventory = 0.0
        self.logger = setup_logger(
            name='maker_back_test',
            log_file='logs/backtest/maker_backtest.log'
        )

    def backtest(self,df:pl.DataFrame,weights:dict,threshold:float=1.5,skew_factor: float = 0.5):
        coef = np.array(weights['coef'])
        means = np.array(weights['scaler_mean']).reshape(-1, 1)
        stds = np.array(weights['scaler_std']).reshape(-1, 1)
        intercept = weights['intercept']
        scale = weights['signal_scale']
        
        bid_prices = df['bid_prices'].to_numpy()
        bid_volumes = df['bid_volumes'].to_numpy()
        ask_prices = df['ask_prices'].to_numpy()
        ask_volumes = df['ask_volumes'].to_numpy()
        vamp_bias = df['vamp_bias'].to_numpy()
        ofi = df['ofi'].to_numpy()
        imbalance = df['imbalance'].to_numpy()
        htf_trend_ratio = df['htf_trend_ratio'].to_numpy()
        dist_to_support = df['dist_to_support'].to_numpy()
        is_sweep = df['is_sweep'].to_numpy()
        buy_vols = df['buy_vol_1s'].to_numpy()
        sell_vols = df['sell_vol_1s'].to_numpy()
        bid_depth_1 = df['bid_volumes'].list.get(0).to_numpy()
        ask_depth_1 = df['ask_volumes'].list.get(0).to_numpy()
        norm_net_vol = df['norm_net_vol'].to_numpy()
        trade_drift_bp = df['trade_drift_bp'].to_numpy()
        fast_vol = df['fast_vol'].to_numpy()
        slow_vol = df['slow_vol'].to_numpy()
        rsi = df['rsi'].to_numpy()
        macd_hist = df['macd_hist'].to_numpy()
        ema50 = df['ema50'].to_numpy()
        vol_ma = df['vol_ma'].to_numpy()
        atr = df['atr'].to_numpy()

        features_val = np.array([vamp_bias,ofi,imbalance,htf_trend_ratio,dist_to_support,is_sweep])

        norm_features = (features_val - means) / stds

        raw_pred = np.dot(coef,norm_features) + intercept

        z_score = raw_pred / scale

        mid = df['mid_price'].to_numpy()
        spread = df['spread'].to_numpy()
        
        n = len(df)

        inventory = np.zeros(n)
        pnl = np.zeros(n)
        bid_price_history = np.zeros(n)
        ask_price_history = np.zeros(n)
        current_inv = 0.0
        # 总成本系数 = (手续费 + 滑点) / 10000
        cost_ratio = (self.market_fee_bp + self.slippage_bp) / 10000

        trades_side = np.zeros(n) # 1 for buy, -1 for sell

        pnl_sum = 0.0

        for i in range(n - 1):
            bid_slope = (bid_volumes[i][0:5].sum()) / (bid_prices[i][0] - bid_prices[i][4])
            ask_slope = (ask_volumes[i][0:5].sum()) / (ask_prices[i][0] - ask_prices[i][4])

            is_fragile = (bid_slope < ask_slope * 0.2)
            if is_fragile:
                self.logger.info(f"round {i} 警惕暴跌")

            alpha = np.clip(z_score[i],-2,2)

            alpha_skew = alpha * spread[i] * 0.1

            vol_ratio = fast_vol[i] / slow_vol[i] if slow_vol[i] > 0 else 0
            dynamic_threshold = threshold * vol_ratio
            # A. 计算报价 (Base Spread + Inventory Skew)
            # 仓位越多，越倾向于卖出：降低 Ask 吸引成交，降低 Bid 防止成交
            inv_ratio = current_inv / self.max_inventory
            queue_ratio = 0.3 + 0.4 * abs(inv_ratio)
            #计算库存偏置 信号化(1,-1,0) * (绝对值(持有库存比) ** 1.5次方) * 灵敏度系数，数值越大机器人越敏感
            #dynamic_skew = np.sign(inv_ratio) * (abs(inv_ratio) ** 1.5) * skew_factor
            inventory_skew = inv_ratio * skew_factor * spread[i]
            inventory_widen = abs(inv_ratio) * spread[i]

            base_half_spread = spread[i] / 2 + 0.1 * spread[i]
            
            #计算买单价 买卖中间价 - (价差 / 2)刚好到达买一价 - (库存偏置 * 最小调整系数通常为0.1): 价格越小，买单就藏得更深，更难成交
            bid_price = mid[i] - base_half_spread - inventory_widen - inventory_skew + alpha_skew
            #计算卖单价 买卖中间价 + (价差 / 2)刚好到达卖一价 - (库存偏置 * 最小调整系数通常为0.1): 价格越小，卖单就越顶到前面，越容易成交
            ask_price = mid[i] + base_half_spread + inventory_widen + inventory_skew + alpha_skew

            if z_score[i] <= -3:
                bid_price = 0
                ask_price = mid[i] + 0.1 * spread[i]
            elif z_score[i] >= 3:
                ask_price = 0  # 撤卖单
                bid_price = mid[i] - 0.1 * spread[i]

            bid_price_history[i] = bid_price
            ask_price_history[i] = ask_price


            #向下取整并且把交易金额修正位可调整一位数
            # bid_price = np.floor(bid_price / self.tick_size) * self.tick_size
            #向上取整并且把交易金额修正位可调整一位数
            # ask_price = np.ceil(ask_price / self.tick_size) * self.tick_size

            volume_enough_buy = (sell_vols[i+1] > bid_depth_1[i] * queue_ratio)
            prob_fill_buy = sell_vols[i+1] / (bid_depth_1[i] + 1e-6)
            volume_enough_sell = (buy_vols[i+1] > ask_depth_1[i] * queue_ratio)
            prob_fill_sell = buy_vols[i+1] / (ask_depth_1[i] + 1e-6)

            price_touch_buy = mid[i+1] <= bid_price if bid_price > 0 else False
            price_touch_sell = mid[i+1] >= ask_price if ask_price > 0 else False

            toxic_buy = trade_drift_bp[i] < -0.2
            toxic_sell = trade_drift_bp[i] > 0.2

            filled_buy = (np.random.rand() < prob_fill_buy and price_touch_buy  and not toxic_buy)
            filled_sell = (np.random.rand() < prob_fill_sell and price_touch_sell and not toxic_sell)

            step_realized_pnl = 0
            if filled_buy and current_inv < self.max_inventory and bid_price > 0:
                current_inv += 1              
                step_realized_pnl -= bid_price
                trades_side[i] = 1

            if filled_sell and current_inv > -self.max_inventory and ask_price > 0:
                current_inv -= 1
                step_realized_pnl += ask_price
                trades_side[i] = -1
            
            # E. 计算盯市收益 (Mark-to-Market PnL)
            inventory[i+1] = current_inv
            # 总收益 = 已实现收益 + 仓位价值变动
            current_unrealized_pnl = current_inv * mid[i+1]
            pnl_sum += step_realized_pnl
            pnl[i+1] = pnl_sum + current_unrealized_pnl

        df_bt = df.with_columns([
            pl.Series("z_score", z_score),
            pl.Series("inventory", inventory),
            pl.Series("step_pnl", pnl),
            pl.Series("trade_side", trades_side),
            pl.Series("my_bid_prices",bid_price_history),
            pl.Series("my_ask_prices",ask_price_history)
        ])

        # print(f"Final Cash Flow: {pnl_sum}")
        # print(f"Final Inventory Value: {current_inv * mid[-1]}")
        # print(f"Min Pnl in series: {np.min(pnl)}")
        # print(f"Max PnL in series: {np.max(pnl)}")
        
        return df_bt
    