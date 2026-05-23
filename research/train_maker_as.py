from src.utils.logger import setup_logger

import polars as pl
import numpy as np
from datetime import datetime

class TrainMakerAS:
    def __init__(self,gamma=0.1,kappa=1.5,sigma_window=100):
        self.gamma = gamma
        self.kappa = kappa
        self.sigma_window = sigma_window
        self.fee_maker = 0.0002
        self.fee_taker = 0.0005

        self.logger = setup_logger(
            name='maker_backtest_v1',
            log_file='logs/backtest/maker_backtest_v1.log',
            fmt='%(message)s',
            clear_on_start=True
        )

        # 初始化状态变量
        self.inventory = 0.0          # 当前库存 q
        self.cash = 0.0               # 现金
        self.pnl = []                 # 权益曲线

        # 🚨 Taker 仓位锁 (核心：同一时间只允许持有一笔趋势单)
        self.is_holding = False       
        self.position_side = 0        # 1: 多头, -1: 空头
        self.entry_price = 0.0        # 记录开仓价格
        self.entry_timestamp = 0
        self.cooldown_timer = 0       # 平仓后的强行冷静期 (Tick数)

        # 🚨 动态参数映射权重 (这些参数需要你后面通过回测微调)
        self.w_obi = 3.0              # OBI对价格偏置的权重 (USDT/单位OBI)
        self.w_basis = 0.2            # 期现基差传导权重 (0.5 表示基差扩大10刀，现货做市价格拔高5刀)
        self.w_mom = 0.01             # 期货动量权重 (USDT/手)

    def run_backtest_v2(self,df_orderbook:pl.DataFrame,df_trades:pl.DataFrame):
        df_orderbook = df_orderbook.with_columns([
            pl.col('timestamp').shift(-1).alias('next_timestamp'),
            pl.col('mid_price').rolling_std(window_size=self.sigma_window).alias('sigma_raw')
        ])

        df_orderbook = df_orderbook.with_columns([
            pl.col('sigma_raw').fill_null(strategy='forward').fill_null(1.0).alias('sigma')
        ])

        df_orderbook = df_orderbook.drop_nulls()

        open_timestamp = 0  # 记录开仓时间
        taker_entry_price = 0.0 # 记录开仓价格
        
        # 定义 Taker 开启的硬门槛 (考虑手续费万5，BTC 60000 刀时单边 30 刀，阈值设为 45 刀)
        threshold = 40.0
        max_hold_time = 2500        # 缩短至 2.5 秒，高频衰竭极快
        stop_loss_limit = 60.0      # 严格截断亏损
        stop_profit_limit = 120.0   # 🔥 新增：微观硬止盈！一旦覆盖完手续费还有赚，立刻落袋！
        current_mid_price = 0.0

        for row in df_orderbook.iter_rows(named=True):
            # 维护冷却计数器
            if self.taker_cooldown > 0:
                self.taker_cooldown -= 1

            s = row['mid_price']

            entry_threshold = threshold * (1 + (self.inventory * 0.1)) 

            # ==========================================
            # 核心风控层：任何模式开启前，先检查有无触发恐慌爆仓
            # ==========================================
            if self.inventory >= self.panic_inventory:
                liquidate_size = self.inventory
                self.inventory = 0.0
                self.cash += s * liquidate_size * (1 - self.fee_taker)
                self.logger.warning(f"[🚨 PANIC LONG CUT] 库存 {liquidate_size}，市价全清！价格: {s}")
                self.pnl.append(self.cash + self.inventory * s)
                continue
            elif self.inventory <= -self.panic_inventory:
                liquidate_size = abs(self.inventory)
                self.inventory = 0.0
                self.cash -= s * liquidate_size * (1 + self.fee_taker)
                self.logger.warning(f"[🚨 PANIC SHORT CUT] 空头爆仓 {liquidate_size}，市价追高买回！价格: {s}")
                self.pnl.append(self.cash + self.inventory * s)
                continue

            current_obi = row['obi_l5']
            current_basis = row['future_spot_basis']
            current_mom = row['future_momentum_1s']

            raw_alpha = (current_obi * self.w_obi) + (current_basis * self.w_basis) + (current_mom * self.w_mom)

            alpha_signal = np.clip(raw_alpha,-10.0,10.0) * 6.0

            if current_mid_price != s:
                if current_mid_price > s:
                    flag = '📉'
                else:
                    flag = '📈'
                current_mid_price = s
                self.logger.info(f"timestamp: {row['timestamp']} | current mid price: {current_mid_price:.2f} | {flag}")
                self.logger.info(f"signal: {alpha_signal:.2f} | obi: {current_obi:.2f} | basis: {current_basis:.2f} | future_mom_1s: {current_mom:.2f}")

            if self.inventory < self.max_inventory and self.inventory > -self.max_inventory:
                if self.taker_cooldown > 0:
                    continue

                is_trending = row.get('is_trending_regime', True)

                if is_trending:
                    if alpha_signal > entry_threshold:
                        self.logger.info(f"[Taker Open Buy] Signal: {alpha_signal:.2f}")
                        # 吃卖一价
                        self.execute_taker_buy(price=row['ask_prices'][0])
                        open_timestamp = row['timestamp']
                        taker_entry_price = row['ask_prices'][0]
                        self.taker_cooldown = 10

                    elif alpha_signal < -entry_threshold:
                        self.logger.info(f"[Taker Open Sell] Signal: {alpha_signal:.2f}")
                        # 吃买一价
                        self.execute_taker_sell(price=row['bid_prices'][0])
                        open_timestamp = row['timestamp']
                        taker_entry_price = row['bid_prices'][0]
                        self.taker_cooldown = 10

            # else:
            time_elapsed = row['timestamp'] - open_timestamp
            should_close = False
            reason = ""

            if self.inventory > 0:
                current_profit = row['bid_prices'][0] - taker_entry_price

                if current_profit >= stop_profit_limit:
                    should_close = True
                    reason = "🔥 Take Profit"
                # elif alpha_signal < -8.0:
                #     should_close = True
                #     reason = "Signal Reversed"
                # elif time_elapsed > max_hold_time:
                #     should_close = True
                #     reason = "Time Out"
                # elif s < (taker_entry_price - stop_loss_limit):
                #     should_close = True
                #     reason = "Stop Loss"

                if should_close:
                    self.logger.info(f"[Taker Close Long] Reason: {reason} | PnL: {s - taker_entry_price:.2f}")
                    self.execute_taker_sell(price=row['bid_prices'][0]) # 卖出平仓
            elif self.inventory < 0:
                current_profit = taker_entry_price - row['ask_prices'][0]
                
                if current_profit >= stop_profit_limit:
                    should_close = True
                    reason = "🔥 Take Profit"
                # elif alpha_signal > 8.0:
                #     should_close = True
                #     reason = "Signal Reversed"
                # elif time_elapsed > max_hold_time:
                #     should_close = True
                #     reason = "Time Out"
                # elif s > (taker_entry_price + stop_loss_limit):
                #     should_close = True
                #     reason = "Stop Loss"

                if should_close:
                    self.logger.info(f"[Taker Close Short] Reason: {reason} | PnL: {taker_entry_price - s:.2f}")
                    self.execute_taker_buy(price=row['ask_prices'][0]) # 买回平仓

            # if row['is_trending_regime']:
            #     self.cancel_all_maker_orders(row['is_trending_regime'])
            #     if row['future_momentum_1s'] > 0 and self.inventory < self.max_inventory and self.taker_cooldown == 0:
            #         self.logger.info(f"[Take][Buy] timestamp: {row['timestamp']} | future_momentum_1s: {row['future_momentum_1s']}")
            #         self.execute_taker_buy(price=row['ask_prices'][0])
            #         self.taker_cooldown = 10  # 强制冷却 10 个 Tick，不准连续刷单
            #     elif row['future_momentum_1s'] < 0 and self.inventory > -self.max_inventory and self.taker_cooldown == 0:
            #         self.logger.info(f"[Take][Sell] timestamp: {row['timestamp']} | future_momentum_1s: {row['future_momentum_1s']}")
            #         self.execute_taker_sell(price=row['bid_prices'][0])
            #         self.taker_cooldown = 10  # 强制冷却 10 个 Tick
            # else:
                # 恢复普通 Maker 挂单
                # 重设 Taker 方向，允许重回做市
                # if self.inventory == 0:
                #     self.last_taker_side = 0

                # r,bid_half_spread,ask_half_spread = self.calculate_as_spread(row)
                # self.post_maker_bid_ask(row['mid_price'],r,bid_half_spread,ask_half_spread,row['timestamp'],row['next_timestamp'],df_trades)

            # 每个 Tick 结束统一 Mark-to-Market 资产清算
            current_pnl = self.cash + self.inventory * s
            self.pnl.append(current_pnl)

        self.logger.info(f"回测结束。最终库存: {self.inventory}, 最终资产净值 PnL: {self.pnl[-1] if self.pnl else 0}")
        self.evaluate_maker_performance(self.pnl)
        return self.pnl

    def cancel_all_maker_orders(self,is_trending_regime):
        self.logger.info(f"!取消所有bid和ask挂单: {is_trending_regime}")

    def execute_taker_buy(self,price:float):
        self.cash -= price * (1 + self.fee_taker)
        self.inventory += 1
        current_pnl = self.cash + self.inventory * price
        self.pnl.append(current_pnl)
        self.logger.info(f"price: {price} | inventory: {self.inventory} | cash: {self.cash}")

    def execute_taker_sell(self,price:float):
        self.cash += price * (1 - self.fee_taker)
        self.inventory -= 1
        current_pnl = self.cash + self.inventory * price
        self.pnl.append(current_pnl)
        self.logger.info(f"price: {price} | inventory: {self.inventory} | cash: {self.cash}")

    def calculate_as_spread(self,row):
        s = row['mid_price']
        sigma = row['sigma']

        current_obi = row['obi_l5']
        current_basis = row['future_spot_basis']
        current_mom = row['future_momentum_1s']

        if np.isnan(sigma) or sigma == 0:
            sigma = 0.001

        # --- 🛠️ 融合特征的 Alpha 信号计算 ---
        # 信号的正负直接代表了微观盘口未来几秒内上涨或下跌的概率
        raw_alpha = (current_obi * self.w_obi) + (current_basis * self.w_basis) + (current_mom * self.w_mom)

        alpha_signal = np.clip(raw_alpha,-10.0,10.0)

        # --- AS 模型核心计算 ---
        # 计算保留价格 r (假设 T-t = 1 简化)
        inventory_skew_step = 45.0
        w_alpha_skew = 6.0
        # r = s - inventory * self.gamma * sigma
        # w_alpha_skew 代表信号每强一分，做市中枢就跟着大趋势直接漂移 4.5 刀
        r = s - (self.inventory * inventory_skew_step) + (alpha_signal * w_alpha_skew)

        # 计算最优价差
        spread = self.gamma * sigma + (2 / self.gamma) * np.log(1 + self.gamma / self.kappa)
        base_half_spread = spread / 2

        # --- 动态价差控制 (Volatility & Order Flow Imbalance Expansion) ---
        # 当信号非常剧烈（绝对值极大）或者市场波动 sigma 放大时，说明有大资金在砸盘/拉升
        # 我们作为 Maker 必须“拉开价差”进行高额溢价保护，防止被逆向选择
        base_half_spread = 50.0

        # 我们引入信号非对称保护逻辑：
        # 如果 alpha_signal > 0（预测看涨）：市场大概率要涨
        # 此时我们的买单（Bid）可以挂得略微浅一点（容易成交）；但卖单（Ask）必须挂得更深、更远！防止被看涨趋势踩踏！
        if alpha_signal > 0:
            bid_half_spread = base_half_spread - abs(alpha_signal) * 0.3 # 买单稍微激进
            ask_half_spread = base_half_spread + abs(alpha_signal) * 1.5 # 卖单大幅撤远避开有毒流
        else:
            # 如果 alpha_signal < 0（预测看跌）：市场大概率要跌
            # 此时买单（Bid）必须大幅往深水区撤退闪避！！卖单（Ask）可以稍微挂浅
            bid_half_spread = base_half_spread + abs(alpha_signal) * 1.5 # 买单深撤闪避
            ask_half_spread = base_half_spread - abs(alpha_signal) * 0.3 # 卖单微调

        # 强制施加最低半价差底线，防止由于激进微调导致跨不过手续费门槛
        bid_half_spread = max(bid_half_spread,40.0)
        ask_half_spread = max(ask_half_spread,40.0)
        return r,bid_half_spread,ask_half_spread

    def post_maker_bid_ask(self,s:float,r:float,bid_half_spread:float,ask_half_spread:float,timestamp:int,next_timestamp:int,df_trades:pl.DataFrame):
        # 最终挂单价格
        bid_price = r - bid_half_spread
        ask_price = r + ask_half_spread

        if self.inventory >= self.max_inventory:
            bid_price = 0
        elif self.inventory <= -self.max_inventory:
            ask_price = float('inf')

        # 4. 🔥【引入回测严格穿透惩罚垫】🔥
        # 真实排队在 VIP0 阶段极其艰难，强制要求价格必须多穿透 2.5 刀才算成交，击碎擦边球伪收益
        cushion = 2.5

        # --- 成交模拟 (Matching Engine) ---
        # 查找在当前时间戳到下一时间戳之间，市场实际发生 Trades 的最高/最低价
        current_ts = timestamp
        next_ts = next_timestamp

        # 模拟市场实际的 Taker 买卖行为
        # 实际生产中这里需要严密排队，这里提供逻辑骨架：
        market_trades = df_trades.filter((pl.col('timestamp') >= current_ts) & (pl.col('timestamp') <= next_ts))

        if len(market_trades) > 0:
            mkt_max_price = market_trades['price'].max()
            mkt_min_price = market_trades['price'].min()

            # 提取首个成交的实际高频时间戳（直接取序列第一项，安全且快）
            trade_ts = market_trades['timestamp'][0]

            if self.inventory >= 0:
                # 如果市场最低价跌破了我们的买单价，说明我们的买单被 Taker 吃了（成交）
                if mkt_min_price <= (bid_price - cushion):
                    self.inventory += 1.0
                    self.cash -= bid_price * (1 + self.fee_maker)
                    self.logger.info(f"[Make][Buy] timestamp: {trade_ts} | price: {bid_price} | cash: {self.cash} | inventory: {self.inventory}")
                # 如果市场最高价冲破了我们的卖单价，说明我们的卖单成交了
                elif mkt_max_price >= (ask_price + cushion):
                    self.inventory -= 1.0
                    self.cash += ask_price * (1 - self.fee_maker)
                    self.logger.info(f"[Make][Sell] timestamp: {trade_ts} | price: {ask_price} | cash: {self.cash} | inventory: {self.inventory}")
            else:
                if mkt_max_price >= (ask_price + cushion):
                    self.inventory -= 1.0
                    self.cash += ask_price * (1 - self.fee_maker)
                    self.logger.info(f"[Make][Sell] timestamp: {trade_ts} | price: {ask_price:.2f} | cash: {self.cash:.2f} | inv: {self.inventory}")
                elif mkt_min_price <= (bid_price - cushion):
                    self.inventory += 1.0
                    self.cash -= bid_price * (1 + self.fee_maker)
                    self.logger.info(f"[Make][Buy] timestamp: {trade_ts} | price: {bid_price:.2f} | cash: {self.cash:.2f} | inv: {self.inventory}")

    def run_backtest_v1(self,df_orderbook:pl.DataFrame,df_trades:pl.DataFrame):
        """
        由于做市商的库存(q)严重依赖前一步的成交状态，
        这里使用高度优化的逐行/逐状态机模拟（或者在Polars中转换为Numpy/Numba加速）
        """
        # 1. 预计算波动率 (Realized Volatility)
        df_orderbook = df_orderbook.with_columns([
            pl.col('mid_price').rolling_std(window_size=self.sigma_window).alias('sigma_raw')
        ])

        df_orderbook = df_orderbook.with_columns([
            pl.col('sigma_raw').fill_null(strategy='forward').fill_null(1.0).alias('sigma')
        ])

        # 转换为 Numpy 以极速运行状态机循环
        mid_prices = df_orderbook['mid_price'].to_numpy()
        sigmas = df_orderbook['sigma'].to_numpy()
        timestamps = df_orderbook['timestamp'].to_numpy()

        obis = df_orderbook['obi_l5'].fill_nan(0.0).to_numpy()
        basis = df_orderbook['future_spot_basis'].fill_nan(0.0).to_numpy()
        future_moms = df_orderbook['future_momentum_1s'].fill_nan(0.0).to_numpy()

        # 初始化状态变量
        inventory = 0.0          # 当前库存 q
        cash = 0.0               # 现金
        pnl = []                 # 权益曲线
        max_inventory = 1.0      # 严格限制最多持有5个币
        panic_inventory = 2.0   # 超过10个币触发恐慌平仓

        # 🚨 动态参数映射权重 (这些参数需要你后面通过回测微调)
        w_obi = 3.0              # OBI对价格偏置的权重 (USDT/单位OBI)
        w_basis = 0.2            # 期现基差传导权重 (0.5 表示基差扩大10刀，现货做市价格拔高5刀)
        w_mom = 0.01             # 期货动量权重 (USDT/手)

        # 模拟订单流（简化版：检查下一阶段的Trade是否穿过了我们的挂单价）
        # 实际高频回测中，需结合 L2 挂单排队模型
        for i in range(len(mid_prices) - 1):
            s = mid_prices[i]
            sigma = sigmas[i]

            if np.isnan(sigma) or sigma == 0:
                sigma = 0.001

            # 3. 读取当前 Tick 的高频微观特征值
            current_obi = obis[i]          # 范围通常在 [-1.0, 1.0]
            current_basis = basis[i]        # 期现绝对价差，单位 USDT
            current_mom = future_moms[i]    # 1秒内期货多空净成交量

            # --- 🛠️ 融合特征的 Alpha 信号计算 ---
            # 信号的正负直接代表了微观盘口未来几秒内上涨或下跌的概率
            raw_alpha = (current_obi * w_obi) + (current_basis * w_basis) + (current_mom * w_mom)

            alpha_signal = np.clip(raw_alpha,-10.0,10.0)

            # --- AS 模型核心计算 ---
            # 计算保留价格 r (假设 T-t = 1 简化)
            inventory_skew_step = 45.0
            w_alpha_skew = 6.0
            # r = s - inventory * self.gamma * sigma
            # w_alpha_skew 代表信号每强一分，做市中枢就跟着大趋势直接漂移 4.5 刀
            r = s - (inventory * inventory_skew_step) + (alpha_signal * w_alpha_skew)

            # 计算最优价差
            spread = self.gamma * sigma + (2 / self.gamma) * np.log(1 + self.gamma / self.kappa)
            base_half_spread = spread / 2

            # --- 动态价差控制 (Volatility & Order Flow Imbalance Expansion) ---
            # 当信号非常剧烈（绝对值极大）或者市场波动 sigma 放大时，说明有大资金在砸盘/拉升
            # 我们作为 Maker 必须“拉开价差”进行高额溢价保护，防止被逆向选择
            base_half_spread = 50.0

            # 我们引入信号非对称保护逻辑：
            # 如果 alpha_signal > 0（预测看涨）：市场大概率要涨
            # 此时我们的买单（Bid）可以挂得略微浅一点（容易成交）；但卖单（Ask）必须挂得更深、更远！防止被看涨趋势踩踏！
            if alpha_signal > 0:
                bid_half_spread = base_half_spread - abs(alpha_signal) * 0.3 # 买单稍微激进
                ask_half_spread = base_half_spread + abs(alpha_signal) * 1.5 # 卖单大幅撤远避开有毒流
            else:
                # 如果 alpha_signal < 0（预测看跌）：市场大概率要跌
                # 此时买单（Bid）必须大幅往深水区撤退闪避！！卖单（Ask）可以稍微挂浅
                bid_half_spread = base_half_spread + abs(alpha_signal) * 1.5 # 买单深撤闪避
                ask_half_spread = base_half_spread - abs(alpha_signal) * 0.3 # 卖单微调

            # 强制施加最低半价差底线，防止由于激进微调导致跨不过手续费门槛
            bid_half_spread = max(bid_half_spread,40.0)
            ask_half_spread = max(ask_half_spread,40.0)

            # 最终挂单价格
            bid_price = r  - bid_half_spread
            ask_price = r + ask_half_spread

            if inventory >= max_inventory:
                bid_price = 0
            elif inventory <= -max_inventory:
                ask_price = float('inf')

            if inventory >= panic_inventory:
                # 触发多头恐慌：直接以当前 mid_price 甚至更差的价格市价卖出 5 个币
                # 必须承担 Taker 手续费 (假设 self.fee_taker = 0.0004)
                liquidate_size = inventory
                inventory = 0.0
                cash += s * liquidate_size * (1 - self.fee_taker)
                self.logger.warning(f"[🚨 PANIC LONG CUT] 库存 {liquidate_size}，市价全清！价格: {s}")
                continue
            elif inventory <= -panic_inventory:
                liquidate_size = abs(inventory)
                inventory = 0.0
                cash -= s * liquidate_size * (1 + self.fee_taker)
                self.logger.warning(f"[🚨 PANIC SHORT CUT] 空头爆仓 {liquidate_size}，市价追高买回！价格: {s}")
                continue

            # 4. 🔥【引入回测严格穿透惩罚垫】🔥
            # 真实排队在 VIP0 阶段极其艰难，强制要求价格必须多穿透 2.5 刀才算成交，击碎擦边球伪收益
            cushion = 2.5

            # --- 成交模拟 (Matching Engine) ---
            # 查找在当前时间戳到下一时间戳之间，市场实际发生 Trades 的最高/最低价
            current_ts = timestamps[i]
            next_ts = timestamps[i+1]

            # 模拟市场实际的 Taker 买卖行为
            # 实际生产中这里需要严密排队，这里提供逻辑骨架：
            market_trades = df_trades.filter((pl.col('timestamp') >= current_ts) & (pl.col('timestamp') <= next_ts))

            if len(market_trades) > 0:
                mkt_max_price = market_trades['price'].max()
                mkt_min_price = market_trades['price'].min()

                # 提取首个成交的实际高频时间戳（直接取序列第一项，安全且快）
                trade_ts = market_trades['timestamp'][0]

                if inventory >= 0:
                    # 如果市场最低价跌破了我们的买单价，说明我们的买单被 Taker 吃了（成交）
                    if mkt_min_price <= (bid_price - cushion):
                        inventory += 1.0
                        cash -= bid_price * (1 + self.fee_maker)
                        self.logger.info(f"[Buy] timestamp: {trade_ts} | price: {bid_price} | cash: {cash} | inventory: {inventory} | signal: {alpha_signal} | r: {r} | spread: {bid_half_spread} | obi: {current_obi} | basis: {current_basis} | future_mom: {current_mom}")
                    # 如果市场最高价冲破了我们的卖单价，说明我们的卖单成交了
                    elif mkt_max_price >= (ask_price + cushion):
                        inventory -= 1.0
                        cash += ask_price * (1 - self.fee_maker)
                        self.logger.info(f"[Sell] timestamp: {trade_ts} | price: {ask_price} | cash: {cash} | inventory: {inventory} | signal: {alpha_signal} | r: {r} | spread: {ask_half_spread} | obi: {current_obi} | basis: {current_basis} | future_mom: {current_mom}")
                else:
                    if mkt_max_price >= (ask_price + cushion):
                        inventory -= 1.0
                        cash += ask_price * (1 - self.fee_maker)
                        self.logger.info(f"[Sell] timestamp: {trade_ts} | price: {ask_price:.2f} | cash: {cash:.2f} | inv: {inventory} | signal: {alpha_signal} | r: {r} | spread: {ask_half_spread} | obi: {current_obi} | basis: {current_basis} | future_mom: {current_mom}")
                    elif mkt_min_price <= (bid_price - cushion):
                        inventory += 1.0
                        cash -= bid_price * (1 + self.fee_maker)
                        self.logger.info(f"[Buy] timestamp: {trade_ts} | price: {bid_price:.2f} | cash: {cash:.2f} | inv: {inventory} | signal: {alpha_signal} | r: {r} | spread: {bid_half_spread} | obi: {current_obi} | basis: {current_basis} | future_mom: {current_mom}")

            # 结算当前价值 (Mark-to-Market PnL)
            current_pnl = cash + inventory * s
            pnl.append(current_pnl)

        print(f"回测结束。最终库存: {inventory}, 最终资产净值 PnL: {current_pnl}")
        self.evaluate_maker_performance(pnl)
        return pnl
    
    def evaluate_maker_performance(self,pnl_history:list, initial_capital=100000.0):
        """
        pnl_history: 每一个tick结算得到的总资产净值(Cash + Inventory * Mid)列表
        """
        adjusted_pnl = np.array(pnl_history) + initial_capital
        pnl_series = pl.Series("pnl",adjusted_pnl)
        # 计算每次迭代的收益率
        returns = pnl_series.pct_change().drop_nulls().to_numpy()

        # 避免分母为0
        if len(returns) == 0 or np.std(returns) == 0:
            print("数据不足或无波动")
            return
        
        # 1. 简易夏普比率 (假设无风险利率为 0)
        # 高频做市通常以 tick 收益率放大到年化，或者直接看单周期夏普
        sharpe = np.mean(returns) / np.std(returns) * np.sqrt(365 * 24 * 60)

        # 2. 索提诺比率 (只计算负收益的标准差)
        downside_returns = returns[returns < 0]
        downside_std = np.std(downside_returns) if len(downside_returns) > 0 else 1e-8
        sortino = np.mean(returns) / downside_std * np.sqrt(365 * 24 * 60)

        # 3. 最大回撤计算
        cum_max = np.maximum.accumulate(adjusted_pnl)
        drawdowns = (cum_max - adjusted_pnl) /cum_max
        max_dd = np.max(drawdowns)

        print(f"======== 楚格量化风控报告 ========")
        print(f"📈 预期年化夏普比率 (Sharpe): {sharpe:.2f}  (Maker目标: > 3.0)")
        print(f"🛡️ 预期年化索提诺比率 (Sortino): {sortino:.2f} (若大幅低于Sharpe说明单边抗单严重)")
        print(f"📉 资金曲线最大回撤 (MaxDD): {max_dd * 100:.3f}%")
        print(f"=================================")