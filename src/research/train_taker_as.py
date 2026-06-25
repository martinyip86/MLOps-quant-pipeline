from src.utils.logger import setup_logger

import polars as pl
import numpy as np
from datetime import datetime

class TrainTakerAS:
    def __init__(self,gamma=0.1,kappa=1.5,sigma_window=100):
        self.gamma = gamma
        self.kappa = kappa
        self.sigma_window = sigma_window
        self.fee_maker = 0.0002
        self.fee_taker_future = 0.0003
        self.fee_taker_spot = 0.0005

        self.logger = setup_logger(
            name='taker_backtest_v1',
            log_file='logs/backtest/taker_backtest_v1.log',
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

        self.min_signal_to_hold = 40.0   # 信号低于这个就考虑走

    def run_taker_backtest(self,df_orderbook:pl.DataFrame):
        df_orderbook = df_orderbook.with_columns([
            pl.col('timestamp').shift(-1).alias('next_timestamp')
        ]).drop_nulls()

        # 🛠️ 极其严格的 Taker 阈值设定
        # 假设 BTC 60000 刀，单边手续费 30 刀，双边 60 刀。
        # 我们的目标是赚取微观爆发波段，触发阈值必须远超双边手续费。
        min_fee_cost = 60.0  # 双边摩擦硬成本 boundary
        entry_threshold = 55.0 # 信号必须极其强劲才打出 Taker 突击
        
        stop_loss_limit = 25.0   # 🚨 快刀割肉：市场不对立刻平仓（亏损 = 40 + 60手续费 = 100刀）
        stop_profit_limit = 100.0 # 🚨 让利润奔跑：吃大动能（盈利 = 150 - 60手续费 = 90刀）
        max_hold_ticks = 500     # 严格的时间衰变（高频动能一般不超过 1.5 秒）

        current_mid_price = 0.0

        for row in df_orderbook.iter_rows(named=True):
            # 维护冷却计数器
            if self.cooldown_timer > 0:
                self.cooldown_timer -= 1

            s = row['mid_price']

            current_obi20 = row['obi_l20']
            current_obi10 = row['obi_l10']
            current_obi5 = row['obi_l5']
            current_basis = row['basis_pct']
            current_mom_1s = row['future_momentum_1s']
            current_mom_5s = row['future_momentum_5s']
            microprice_dev = row['microprice_dev']
            spread_ma = row['spread_ma']
            spread_zscore = row['spread_zscore']
            mom_bps_1s = row['mom_bps_1s']
            future_spot_basis = row['future_spot_basis']
            basis_ma = row['basis_ma']
            ofi_1s = row['ofi_1s']
            buy_impact = row['buy_impact_bps']

            w_obi = row['obi_l5'] * 0.5 + row['obi_l10'] * 0.3 + row['obi_l20'] * 0.2

            # 计算融合 Alpha 信号
            raw_alpha = (w_obi * 2.0) + (row['ofi_1s'] * 1.5) + (row['future_momentum_1s'] / 10 * 0.8)
            alpha_signal = np.clip(raw_alpha, -10.0, 10.0) * 10.0  # 放大信号灵敏度

            if current_mid_price != s:
                if current_mid_price > s:
                    flag = '📉'
                else:
                    flag = '📈'
                current_mid_price = s
                self.logger.info(f"timestamp: {row['timestamp']} | current mid price: {current_mid_price:.2f} | {flag}")
                self.logger.info(f"----signal: {alpha_signal:.2f} | obi5: {current_obi5:.2f} | obi10: {current_obi10:.2f} | obi20: {current_obi20:.2f}")
                self.logger.info(f"----basis: {current_basis:.2f} | future_mom_1s: {current_mom_1s:.2f} | future_mom_5s: {current_mom_5s:.2f}")
                self.logger.info(f"----microprice_dev:{microprice_dev:.4f} | spread_ma:{spread_ma:.4f} | spread_zscore:{spread_zscore:.4f}")
                self.logger.info(f"----mom_bps_1s:{mom_bps_1s:.2f} | future_spot_basis:{future_spot_basis:.2f} | ofi_1s:{ofi_1s:.2f}")

            # ==========================================
            # 状态机分流：持仓管理 vs 寻机开仓
            # ==========================================
            if self.is_holding:
                # ------ 🚨 严格的持仓出场逻辑 ------
                time_elapsed = row['timestamp'] - self.entry_timestamp
                should_close = False
                reason = ""

                if self.position_side == 1: # 多头仓位
                    qty = 0.1
                    # 用对手价（买一价）计算当前的瞬时账面利润 (未扣除平仓手续费)
                    raw_profit = (row['bid_prices'][0] - self.entry_price) * qty
                    cost = (row['bid_prices'][0] + self.entry_price) * qty * self.fee_taker

                    if raw_profit >= stop_profit_limit:
                        should_close, reason = True, "🔥 趋势脉冲止盈"
                    elif raw_profit <= -stop_loss_limit:
                        should_close, reason = True, "❌ 动能衰竭止损"
                    # elif time_elapsed > max_hold_ticks:
                    #     should_close, reason = True, "⏳ 逻辑超时保护"
                    elif alpha_signal < self.min_signal_to_hold:
                        should_close, reason = True, "信号衰减"

                    if should_close:
                        # 执手平仓：吃买一价卖出
                        self.execute_taker_sell(price=row['bid_prices'][0])
                        net_pnl = raw_profit - cost
                        self.logger.info(f"[Taker Close Long] {reason} | 止盈价: {stop_profit_limit} | 净盈亏: {net_pnl:.2f} | 持时: {time_elapsed}Ticks")

                        # 重置状态，强行进入冷静期，防止在趋势末端反复反复开仓被双杀
                        self.is_holding = False
                        self.position_side = 0
                        self.cooldown_timer = 50

                elif self.position_side == -1:  # 空头仓位
                    qty = 0.1
                    # 用对手价（卖一价）计算利润
                    raw_profit = (self.entry_price - row['ask_prices'][0]) * qty
                    cost = (self.entry_price + row['ask_prices'][0]) * qty * self.fee_taker

                    if raw_profit >= stop_profit_limit:
                        should_close, reason = True, "🔥 趋势脉冲止盈"
                    elif raw_profit <= -stop_loss_limit:
                        should_close, reason = True, "❌ 动能衰竭止损"
                    # elif time_elapsed > max_hold_ticks:
                    #     should_close, reason = True, "⏳ 逻辑超时保护"

                    if should_close:
                        # 执手平仓：吃卖一价买回
                        self.execute_taker_buy(price=row['ask_prices'][0])
                        net_pnl = raw_profit - cost
                        self.logger.info(f"[Taker Close Short] {reason} | 止盈价: {stop_profit_limit}  | 净盈亏: {net_pnl:.2f} | 持时: {time_elapsed}Ticks")

                        # 重置状态，强行进入冷静期，防止在趋势末端反复反复开仓被双杀
                        self.is_holding = False
                        self.position_side = 0
                        self.cooldown_timer = 50

            else:
                # ------ 🚨 寻找高爆发机会开仓 ------
                if self.cooldown_timer > 0:
                    continue

                is_trending = row.get('is_trending_regime', True)

                if is_trending:
                    qty = 0.1
                    # 计算预估的双边开仓硬成本 (USDT)
                    estimated_open_fee = (row['ask_prices'][0] * self.fee_taker_spot + row['bid_prices'][0] * self.fee_taker_future) * qty
                    if alpha_signal > entry_threshold and future_spot_basis > basis_ma + 15.0 and (future_spot_basis - basis_ma) * qty > (estimated_open_fee + buy_impact * 0.1):
                        # 趋势看涨，高频主动吃卖一价开多
                        self.execute_taker_buy(price=row['ask_prices'][0])
                        self.entry_price = row['ask_prices'][0]
                        self.entry_timestamp = row['timestamp']
                        self.is_holding = True
                        self.position_side = 1
                        self.logger.info(f"[🚨 Taker Open Long] 触发! 信号: {alpha_signal:.2f} | OBI: {w_obi:.2f} | OFI: {ofi_1s:.2f} | 开仓价: {self.entry_price} | timestamp: {row['timestamp']}")
                        self.logger.info(f"----signal: {alpha_signal:.2f} | obi5: {current_obi5:.2f} | obi10: {current_obi10:.2f} | obi20: {current_obi20:.2f}")
                        self.logger.info(f"----basis: {current_basis:.2f} | future_mom_1s: {current_mom_1s:.2f} | future_mom_5s: {current_mom_5s:.2f}")
                        self.logger.info(f"----microprice_dev:{microprice_dev:.4f} | spread_ma:{spread_ma:.4f} | spread_zscore:{spread_zscore:.4f}")
                        self.logger.info(f"----mom_bps_1s:{mom_bps_1s:.2f} | future_spot_basis:{future_spot_basis:.2f} | ofi_1s:{ofi_1s:.2f}")

                    elif alpha_signal < -entry_threshold and w_obi < 0.5 and ofi_1s < 0 and microprice_dev < 0:
                        # if current_mom_1s < -15.0 and current_obi20 < -0.85: # 加上了上一课的过滤
                            # 趋势看跌，高频主动吃买一价开空
                            self.execute_taker_sell(price=row['bid_prices'][0])
                            self.entry_price = row['bid_prices'][0]
                            self.entry_timestamp = row['timestamp']
                            self.is_holding = True
                            self.position_side = -1

                            # 🌟 核心：根据入手瞬间的期货 1s 动量，动态计算这笔单子的止盈幅度
                            # 基础止盈设为 30 刀（防震荡快速落袋），动量每多 10 刀，止盈幅度增加 1.5 倍动量值
                            # 例如：如果是你大赚的那笔单子，future_mom_1s 是 -20.75，那么：
                            # 动态止盈 = 30.0 + abs(-20.75) * 2.0 = 71.5 刀
                            # 如果是超级大暴跌（mom达 -60），动态止盈会自动放大到 150 刀！
                            stop_profit_limit = 30.0 + (abs(current_mom_1s) * 2.0)
                            
                            # 限制一个最高止盈上限，防止过度贪婪
                            stop_profit_limit = min(stop_profit_limit, 250.0) 
                            
                            # 同时也可以动态把止损设窄一点，实现高盈亏比
                            self.current_stop_loss = 25.0

                            self.logger.info(f"[🚨 Taker Open Short] 触发! 信号: {alpha_signal:.2f} | OBI: {w_obi:.2f} | OFI: {ofi_1s:.2f} | 开仓价: {self.entry_price} | timestamp: {row['timestamp']}")
                            self.logger.info(f"----signal: {alpha_signal:.2f} | obi5: {current_obi5:.2f} | obi10: {current_obi10:.2f} | obi20: {current_obi20:.2f}")
                            self.logger.info(f"----basis: {current_basis:.2f} | future_mom_1s: {current_mom_1s:.2f} | future_mom_5s: {current_mom_5s:.2f}")
                            self.logger.info(f"----microprice_dev:{microprice_dev:.4f} | spread_ma:{spread_ma:.4f} | spread_zscore:{spread_zscore:.4f}")
                            self.logger.info(f"----mom_bps_1s:{mom_bps_1s:.2f} | future_spot_basis:{future_spot_basis:.2f} | ofi_1s:{ofi_1s:.2f}")

            # 每个 Tick 结束统一 Mark-to-Market 资产清算
            current_pnl = self.cash + self.inventory * s
            self.pnl.append(current_pnl)

        self.logger.info(f"回测结束。最终库存: {self.inventory}, 最终资产净值 PnL: {self.pnl[-1] if self.pnl else 0}")
        self.evaluate_maker_performance(self.pnl)
        return self.pnl

    def cancel_all_maker_orders(self,is_trending_regime):
        self.logger.info(f"!取消所有bid和ask挂单: {is_trending_regime}")

    def execute_taker_buy(self,price:float):
        qty = 0.1
        self.cash -= price * qty * (1 + self.fee_taker)
        self.inventory += qty
        current_pnl = self.cash + self.inventory * price
        self.pnl.append(current_pnl)
        self.logger.info(f"price: {price} | inventory: {self.inventory} | cash: {self.cash}")

    def execute_taker_sell(self,price:float):
        qty = 0.1
        self.cash += price * qty * (1 - self.fee_taker)
        self.inventory -= qty
        current_pnl = self.cash + self.inventory * price
        self.pnl.append(current_pnl)
        self.logger.info(f"price: {price} | inventory: {self.inventory} | cash: {self.cash}")
    
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