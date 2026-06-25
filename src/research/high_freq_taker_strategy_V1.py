import polars as pl
import numpy as np
from datetime import datetime,timezone

from src.utils.logger import setup_logger

class HighFreqTakerStrategy_V1:
    def __init__(self,stop_loss_pct=0.0015,take_profit_pct=0.004,cooldown_steps=200):
        self.stop_loss_pct = stop_loss_pct      # 硬性止损：0.15% (比如BTC波动100刀)
        self.take_profit_pct = take_profit_pct   # 止盈：0.4%
        self.cooldown_steps = cooldown_steps    # 冷静期步数（高频Tick数，比如200个100ms=20秒）

        # 核心玩法参数：单边摩擦成本预估（Taker开仓万2 + Taker平仓万2 + 预估双边滑点万2 = 万6）
        self.min_friction_pct = 0.0006  # 0.06% (这是你的硬性成本墙)
        # 我们要求：只有浮盈超过摩擦成本，且至少赚到万分之 2 的纯利润时（共万8），才允许根据动能信号“提早平仓”
        self.min_profit_threshold = self.min_friction_pct + 0.0002

        # 🌟 修复崩盘的核心参数：微观持仓 Tick 保护墙
        # 既然是以期货为主表，开仓后至少要硬性死扛 20 个 Tick（约 1-2 秒），防止被同秒内的微观信号闪烁误杀
        self.min_holding_ticks = 30

        # 🌟 2. Prop Firm 全局熔断保护闸（针对10万刀账户，每日允许最大亏损设为3.5%（3500刀），留出1.5%的安全垫）
        self.daily_loss_limit_pct = 0.035  # 日内累计亏损若超3.5%，策略直接断电自毁
        self.is_melted = False             # 熔断标志位

        # 状态变量
        self.position = 0           # 当前持仓: 0, 1(多), -1(空)
        self.entry_price = 0.0      # 入场价格
        self.cooldown_timer = 0     # 冷静期计数器
        self.holding_tick_counter = 0  # 🌟 持仓 Tick 计数器

        self.logger = setup_logger(
            name='high_freq_taker_strategy',
            log_file='logs/backtest/high_freq_taker_strategy.log',
            fmt='%(message)s',
            clear_on_start=True
        )

    def run(self,df:pl.DataFrame):
        """
        输入：Polars collect() 后的 DataFrame，包含经过对齐的特征和盘口价
        """
        # 为了快速迭代，这里用 numpy/pandas 演示逐行撮合逻辑
        timestamp = df['timestamp'].to_numpy()
        bid_price_future = df['bid_price_future'].to_numpy()
        ask_price_future = df['ask_price_future'].to_numpy()
        mid_price = df['mid_price_future'].to_numpy()

        # 引入开仓时的盘口价差过滤
        # 如果价差大于万分之 3，说明流动性不好，Taker进去直接亏完，坚决不开
        spread_future = df['spread_future'].to_numpy()
        buy_impact_bps = df['buy_impact_bps_future'].to_numpy()

        # 信号
        sig_long = df['signal_long'].to_numpy()
        sig_short = df['signal_short'].to_numpy()
        future_ofi_1s = df['future_ofi_1s'].to_numpy()
        future_obi_l5 = df['future_obi_l5'].to_numpy()
        spot_ofi_2s = df['spot_ofi_2s'].to_numpy()
        spot_obi_l5 = df['spot_obi_l5'].to_numpy()
        oi_momentum = df['oi_momentum'].to_numpy()
        future_spot_basis = df['future_spot_basis'].to_numpy()
        premium_discount = df['premium_discount'].to_numpy()

        # 未使用属性
        future_ofi_5s = df['future_ofi_5s'].to_numpy()
        spot_obi_l10 = df['spot_obi_l10'].to_numpy()
        spot_obi_l20 = df['spot_obi_l20'].to_numpy()
        future_obi_l20 = df['future_obi_l20'].to_numpy()
        future_obi_l10 = df['future_obi_l10'].to_numpy()
        basis_ma = df['basis_ma'].to_numpy()
        microprice_dev_spot = df['microprice_dev_spot'].to_numpy()
        spread_ma_spot = df['spread_ma_spot'].to_numpy()
        spot_spread_zscore = df['spot_spread_zscore'].to_numpy()
        ofi_bps_1s = df['ofi_bps_1s'].to_numpy()

        pnl = []
        current_pnl = 0.0
        current_mid_price = 0.0
        # 记录日内最高净值，用于计算回撤风险
        max_pnl_tracked = 0.0

        for i in range(len(df)):
            # 🌟 亏损熔断安全大闸：最高优先级
            # 如果从日内最高点回撤超过了指定的 daily_loss_limit_pct，直接拒绝执行后续所有逻辑
            if (max_pnl_tracked - current_pnl) > self.daily_loss_limit_pct:
                if not self.is_melted:
                    self.logger.warning(f"🚨🚨 [CIRCUIT BREAKER TRIGGERED] 触发日内最大回撤保护！当前累计回撤: {max_pnl_tracked - current_pnl:.4f}. 策略断电熔断！")
                    self.is_melted = True
                pnl.append(current_pnl)
                break
            # 1. 冷静期倒计时
            if self.cooldown_timer > 0:
                self.cooldown_timer -= 1

            dt = datetime.fromtimestamp(timestamp[i] / 1000,tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            s = mid_price[i]

            if current_mid_price != s:
                if current_mid_price > s:
                    flag = '📉'
                else:
                    flag = '📈'
                current_mid_price = s
                self.logger.info(f"{dt} current mid price: {current_mid_price:.2f} | {flag}")
                self.logger.info(f"----future_ofi_1s:{future_ofi_1s[i]:.4f} | future_ofi_5s:{future_ofi_5s[i]:.4f} | spot_ofi_2s:{spot_ofi_2s[i]:.4f} | ofi_bps_1s:{ofi_bps_1s[i]:.4f}")
                self.logger.info(f"----future_obi_l5:{future_obi_l5[i]:.4f} | future_obi_l10:{future_obi_l10[i]:.4f} | future_obi_l20:{future_obi_l20[i]:.4f}")
                self.logger.info(f"----spot_obi_l5:{spot_obi_l5[i]:.4f} | spot_obi_l10:{spot_obi_l10[i]:.4f} | spot_obi_l20:{spot_obi_l20[i]}")
                self.logger.info(f"----basis_ma:{basis_ma[i]:.4f} | future_spot_basis:{future_spot_basis[i]:.4f} | spot_spread_zscore: {spot_spread_zscore[i]:.4f} | spread_ma_spot:{spread_ma_spot[i]:.4f}")
                self.logger.info(f"----premium_discount: {premium_discount[i]:.4f} | oi_momentum:{oi_momentum[i]:.4f} | microprice_dev_spot: {microprice_dev_spot[i]:.4f}")

            # 2. 如果当前有持仓，先检查止损和止盈
            if self.position != 0:
                self.holding_tick_counter += 1
                current_price = ask_price_future[i] if self.position == 1 else bid_price_future[i]

                # 计算浮动盈亏
                if self.position == 1:
                    return_pct = (current_price - self.entry_price) / self.entry_price
                else:
                    return_pct = (self.entry_price - current_price) / self.entry_price

                # 检查是否触发硬性止损或止盈
                if return_pct <= -self.stop_loss_pct:
                    # 触发止损：立刻 Taker 平仓
                    current_pnl += return_pct - self.min_friction_pct
                    self.position = 0
                    self.cooldown_timer = self.cooldown_steps  # 触发冷静期
                    pnl.append(current_pnl)
                    self.logger.info(f"{dt} [Taker Stop Loss Close] price:{return_pct - self.min_friction_pct} | pnl:{current_pnl}")
                    self.logger.info(f"----future_ofi_1s:{future_ofi_1s[i]:.4f} | future_ofi_5s:{future_ofi_5s[i]:.4f} | spot_ofi_2s:{spot_ofi_2s[i]:.4f} | ofi_bps_1s:{ofi_bps_1s[i]:.4f}")
                    self.logger.info(f"----future_obi_l5:{future_obi_l5[i]:.4f} | future_obi_l10:{future_obi_l10[i]:.4f} | future_obi_l20:{future_obi_l20[i]:.4f}")
                    self.logger.info(f"----spot_obi_l5:{spot_obi_l5[i]:.4f} | spot_obi_l10:{spot_obi_l10[i]:.4f} | spot_obi_l20:{spot_obi_l20[i]}")
                    self.logger.info(f"----basis_ma:{basis_ma[i]:.4f} | future_spot_basis:{future_spot_basis[i]:.4f} | spot_spread_zscore: {spot_spread_zscore[i]:.4f} | spread_ma_spot:{spread_ma_spot[i]:.4f}")
                    self.logger.info(f"----premium_discount: {premium_discount[i]:.4f} | oi_momentum:{oi_momentum[i]:.4f} | microprice_dev_spot: {microprice_dev_spot[i]:.4f}")
                    continue
                elif return_pct >= self.take_profit_pct:
                    # 触发止盈
                    current_pnl += return_pct - self.min_friction_pct
                    self.position = 0
                    pnl.append(current_pnl)
                    self.logger.info(f"{dt} [Taker Take Profit Close] price:{return_pct - self.min_friction_pct} | pnl:{current_pnl}")
                    self.logger.info(f"----future_ofi_1s:{future_ofi_1s[i]:.4f} | future_ofi_5s:{future_ofi_5s[i]:.4f} | spot_ofi_2s:{spot_ofi_2s[i]:.4f} | ofi_bps_1s:{ofi_bps_1s[i]:.4f}")
                    self.logger.info(f"----future_obi_l5:{future_obi_l5[i]:.4f} | future_obi_l10:{future_obi_l10[i]:.4f} | future_obi_l20:{future_obi_l20[i]:.4f}")
                    self.logger.info(f"----spot_obi_l5:{spot_obi_l5[i]:.4f} | spot_obi_l10:{spot_obi_l10[i]:.4f} | spot_obi_l20:{spot_obi_l20[i]}")
                    self.logger.info(f"----basis_ma:{basis_ma[i]:.4f} | future_spot_basis:{future_spot_basis[i]:.4f} | spot_spread_zscore: {spot_spread_zscore[i]:.4f} | spread_ma_spot:{spread_ma_spot[i]:.4f}")
                    self.logger.info(f"----premium_discount: {premium_discount[i]:.4f} | oi_momentum:{oi_momentum[i]:.4f} | microprice_dev_spot: {microprice_dev_spot[i]:.4f}")
                    continue

               # C. 🌟 核心玩法修正：动能衰减平仓（必须满足盈利门槛！）
                # 只有当当前的利润已经【铁定覆盖双边手续费与滑点】，并且有富余时，才允许看动能信号脸色出场
                if self.holding_tick_counter >= self.min_holding_ticks and return_pct >= self.min_profit_threshold:
                    # 利润足够了，此时如果发现动能反转（比如OFI掉头），立刻锁住利润闪电平仓
                    if self.position == 1 and future_ofi_1s[i] < -25.0:
                        current_pnl += (return_pct - self.min_friction_pct)
                        self.position = 0
                        self.cooldown_timer = self.cooldown_steps
                        pnl.append(current_pnl)
                        self.logger.info(f"{dt} [Taker Signal Down Close Buy] price:{return_pct - self.min_friction_pct} | pnl:{current_pnl}")
                        self.logger.info(f"----future_ofi_1s:{future_ofi_1s[i]:.4f} | future_ofi_5s:{future_ofi_5s[i]:.4f} | spot_ofi_2s:{spot_ofi_2s[i]:.4f} | ofi_bps_1s:{ofi_bps_1s[i]:.4f}")
                        self.logger.info(f"----future_obi_l5:{future_obi_l5[i]:.4f} | future_obi_l10:{future_obi_l10[i]:.4f} | future_obi_l20:{future_obi_l20[i]:.4f}")
                        self.logger.info(f"----spot_obi_l5:{spot_obi_l5[i]:.4f} | spot_obi_l10:{spot_obi_l10[i]:.4f} | spot_obi_l20:{spot_obi_l20[i]}")
                        self.logger.info(f"----basis_ma:{basis_ma[i]:.4f} | future_spot_basis:{future_spot_basis[i]:.4f} | spot_spread_zscore: {spot_spread_zscore[i]:.4f} | spread_ma_spot:{spread_ma_spot[i]:.4f}")
                        self.logger.info(f"----premium_discount: {premium_discount[i]:.4f} | oi_momentum:{oi_momentum[i]:.4f} | microprice_dev_spot: {microprice_dev_spot[i]:.4f}")
                        continue
                    elif self.position == -1 and future_ofi_1s[i] > 25.0:
                        current_pnl += (return_pct - self.min_friction_pct)
                        self.position = 0
                        self.cooldown_timer = self.cooldown_steps
                        pnl.append(current_pnl)
                        self.logger.info(f"{dt} [Taker Signal Down Close Sell] price:{return_pct - self.min_friction_pct} | pnl:{current_pnl}")
                        self.logger.info(f"----future_ofi_1s:{future_ofi_1s[i]:.4f} | future_ofi_5s:{future_ofi_5s[i]:.4f} | spot_ofi_2s:{spot_ofi_2s[i]:.4f} | ofi_bps_1s:{ofi_bps_1s[i]:.4f}")
                        self.logger.info(f"----future_obi_l5:{future_obi_l5[i]:.4f} | future_obi_l10:{future_obi_l10[i]:.4f} | future_obi_l20:{future_obi_l20[i]:.4f}")
                        self.logger.info(f"----spot_obi_l5:{spot_obi_l5[i]:.4f} | spot_obi_l10:{spot_obi_l10[i]:.4f} | spot_obi_l20:{spot_obi_l20[i]}")
                        self.logger.info(f"----basis_ma:{basis_ma[i]:.4f} | future_spot_basis:{future_spot_basis[i]:.4f} | spot_spread_zscore: {spot_spread_zscore[i]:.4f} | spread_ma_spot:{spread_ma_spot[i]:.4f}")
                        self.logger.info(f"----premium_discount: {premium_discount[i]:.4f} | oi_momentum:{oi_momentum[i]:.4f} | microprice_dev_spot: {microprice_dev_spot[i]:.4f}")
                        continue
                else:
                    # 💡 关键哲学：如果 raw_return 还不能覆盖手续费和滑点，
                    # 就算 sig_long 变成了 0，我们也绝不主动平仓！因为现在平仓是【铁定亏手续费】，
                    # 我们选择让子弹再飞一会儿，要么它冲过去触发止盈或满足最低利润，要么老老实实被打硬止损。
                    pass

            # 3. 如果没有持仓且不在冷静期，检查开仓信号
            if self.position == 0 and self.cooldown_timer == 0:
                # 检查盘口摩擦，如果当前盘口拉得很稀（Spread太大），放弃本次冲锋
                current_spread_bps = spread_future[i] / ask_price_future[i]
                if current_spread_bps > 0.0003: # 价差大于万3，不划算
                    pnl.append(current_pnl)
                    continue

                # 🌟 计算动态冲击成本滑点墙 (Impact Slippage Cushion)
                # 如果特征里包含真实的冲击成本特征（buy_impact_bps），则将其转换为百分比；
                # 若无，则使用当前价差的 1.5 倍作为动态高频 Taker 撞单滑点预估垫片。
                if buy_impact_bps is not None:
                    estimated_slippage_pct = buy_impact_bps[i] / 10000.0
                else:
                    estimated_slippage_pct = current_spread_bps * 1.5

                if sig_long[i] == 1:
                    # Taker 买入：吃掉对方的卖一价 (Ask Price)
                    self.position = 1
                    self.entry_price = ask_price_future[i] * (1.0 + estimated_slippage_pct)
                    self.logger.info(f"{dt} [Taker Open Buy] price:{ask_price_future[i]}")
                    self.logger.info(f"----future_ofi_1s:{future_ofi_1s[i]:.4f} | future_ofi_5s:{future_ofi_5s[i]:.4f} | spot_ofi_2s:{spot_ofi_2s[i]:.4f} | ofi_bps_1s:{ofi_bps_1s[i]:.4f}")
                    self.logger.info(f"----future_obi_l5:{future_obi_l5[i]:.4f} | future_obi_l10:{future_obi_l10[i]:.4f} | future_obi_l20:{future_obi_l20[i]:.4f}")
                    self.logger.info(f"----spot_obi_l5:{spot_obi_l5[i]:.4f} | spot_obi_l10:{spot_obi_l10[i]:.4f} | spot_obi_l20:{spot_obi_l20[i]}")
                    self.logger.info(f"----basis_ma:{basis_ma[i]:.4f} | future_spot_basis:{future_spot_basis[i]:.4f} | spot_spread_zscore: {spot_spread_zscore[i]:.4f} | spread_ma_spot:{spread_ma_spot[i]:.4f}")
                    self.logger.info(f"----premium_discount: {premium_discount[i]:.4f} | oi_momentum:{oi_momentum[i]:.4f} | microprice_dev_spot: {microprice_dev_spot[i]:.4f}")
                elif sig_short[i] == -1:
                    # Taker 卖出：吃掉对方的买一价 (Bid Price)
                    self.position = -1
                    self.entry_price = bid_price_future[i] * (1.0 - estimated_slippage_pct)
                    self.logger.info(f"{dt} [Taker Open Sell] price:{bid_price_future[i]}")
                    self.logger.info(f"----future_ofi_1s:{future_ofi_1s[i]:.4f} | future_ofi_5s:{future_ofi_5s[i]:.4f} | spot_ofi_2s:{spot_ofi_2s[i]:.4f} | ofi_bps_1s:{ofi_bps_1s[i]:.4f}")
                    self.logger.info(f"----future_obi_l5:{future_obi_l5[i]:.4f} | future_obi_l10:{future_obi_l10[i]:.4f} | future_obi_l20:{future_obi_l20[i]:.4f}")
                    self.logger.info(f"----spot_obi_l5:{spot_obi_l5[i]:.4f} | spot_obi_l10:{spot_obi_l10[i]:.4f} | spot_obi_l20:{spot_obi_l20[i]}")
                    self.logger.info(f"----basis_ma:{basis_ma[i]:.4f} | future_spot_basis:{future_spot_basis[i]:.4f} | spot_spread_zscore: {spot_spread_zscore[i]:.4f} | spread_ma_spot:{spread_ma_spot[i]:.4f}")
                    self.logger.info(f"----premium_discount: {premium_discount[i]:.4f} | oi_momentum:{oi_momentum[i]:.4f} | microprice_dev_spot: {microprice_dev_spot[i]:.4f}")

            # 动态更新日内最高净值水位线，用于防回撤熔断
            if current_pnl > max_pnl_tracked:
                max_pnl_tracked = current_pnl

        self.logger.info(f"Last Pnl: {pnl[-1]}")
