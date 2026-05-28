import polars as pl
import numpy as np
from datetime import datetime,timezone

from src.utils.logger import setup_logger

class HighFreqTakerStrategy:
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

        self.pnl = []
        self.current_pnl = 0.0
        self.current_mid_price = 0.0
        # 记录日内最高净值，用于计算回撤风险
        self.max_pnl_tracked = 0.0

        self.logger = setup_logger(
            name='high_freq_taker_strategy',
            log_file='logs/backtest/high_freq_taker_strategy.log',
            fmt='%(message)s',
            clear_on_start=True
        )

    def run(self,df:pl.DataFrame):
        # 🌟 亏损熔断安全大闸：最高优先级
        # 如果从日内最高点回撤超过了指定的 daily_loss_limit_pct，直接拒绝执行后续所有逻辑
        if (self.max_pnl_tracked - self.current_pnl) > self.daily_loss_limit_pct:
            if not self.is_melted:
                self.logger.warning(f"🚨🚨 [CIRCUIT BREAKER TRIGGERED] 触发日内最大回撤保护！当前累计回撤: {self.max_pnl_tracked - self.current_pnl:.4f}. 策略断电熔断！")
                self.is_melted = True
            return
        
        # 2. 🌟 极速标量提取：直接把这 1 行转为 Python dict，规避 NumPy 数组切片开销
        row = df.row(0, named=True)

        # 为了快速迭代，这里用 numpy/pandas 演示逐行撮合逻辑
        timestamp = row['timestamp']
        bid_price_future = row['bid_price_future']
        ask_price_future = row['ask_price_future']
        mid_price = row['mid_price_future']

        # 引入开仓时的盘口价差过滤
        # 如果价差大于万分之 3，说明流动性不好，Taker进去直接亏完，坚决不开
        spread_future = row['spread_future']
        buy_impact_bps = row['buy_impact_bps_future']
        # 信号
        sig_long = row['signal_long']
        sig_short = row['signal_short']
        future_ofi_1s = row['future_ofi_1s']
        future_obi_l5 = row['future_obi_l5']
        spot_ofi_2s = row['spot_ofi_2s']
        spot_obi_l5 = row['spot_obi_l5']
        oi_momentum = row['oi_momentum']
        future_spot_basis = row['future_spot_basis']
        premium_discount = row['premium_discount']
        # 1. 冷静期倒计时
        if self.cooldown_timer > 0:
            self.cooldown_timer -= 1

        dt = datetime.fromtimestamp(timestamp / 1000,tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        if self.current_mid_price != mid_price:
            flag = '📉' if self.current_mid_price > mid_price else '📈'
            self.current_mid_price = mid_price
            self.logger.info(f"{dt} current mid price: {self.current_mid_price:.2f} | {flag}")
            self.logger.info(f"----future_ofi_1s:{future_ofi_1s} | future_obi_l5:{future_obi_l5}")
            self.logger.info(f"----spot_ofi_2s:{spot_ofi_2s} | spot_obi_l5:{spot_obi_l5}")
            self.logger.info(f"----oi_momentum:{oi_momentum} | future_spot_basis:{future_spot_basis}")
            self.logger.info(f"----premium_discount: {premium_discount}")

        # 2. 如果当前有持仓，先检查止损和止盈
        if self.position != 0:
            self.holding_tick_counter += 1
            current_price = ask_price_future if self.position == 1 else bid_price_future

            # 计算浮动盈亏
            return_pct = (current_price - self.entry_price) / self.entry_price if self.position == 1 else (self.entry_price - current_price) / self.entry_price

            # A. 硬止损检查
            if return_pct <= -self.stop_loss_pct:
                # 触发止损：立刻 Taker 平仓
                self.current_pnl += return_pct - self.min_friction_pct
                self.position = 0
                self.cooldown_timer = self.cooldown_steps  # 触发冷静期
                self.pnl.append(self.current_pnl)
                self.logger.info(f"{dt} [LIVE STOP LOSS CLOSE] price:{return_pct - self.min_friction_pct} | pnl:{self.current_pnl}")
                self.logger.info(f"----future_ofi_1s:{future_ofi_1s} | future_obi_l5:{future_obi_l5}")
                self.logger.info(f"----spot_ofi_2s:{spot_ofi_2s} | spot_obi_l5:{spot_obi_l5}")
                self.logger.info(f"----oi_momentum:{oi_momentum} | future_spot_basis:{future_spot_basis}")
                self.logger.info(f"----premium_discount: {premium_discount}")
                return
            
            # B. 硬止盈检查
            elif return_pct >= self.take_profit_pct:
                # 触发止盈
                self.current_pnl += return_pct - self.min_friction_pct
                self.position = 0
                self.pnl.append(self.current_pnl)
                self.logger.info(f"{dt} [LIVE TAKE PROFIT CLOSE] price:{return_pct - self.min_friction_pct} | pnl:{self.current_pnl}")
                self.logger.info(f"----future_ofi_1s:{future_ofi_1s} | future_obi_l5:{future_obi_l5}")
                self.logger.info(f"----spot_ofi_2s:{spot_ofi_2s} | spot_obi_l5:{spot_obi_l5}")
                self.logger.info(f"----oi_momentum:{oi_momentum} | future_spot_basis:{future_spot_basis}")
                self.logger.info(f"----premium_discount: {premium_discount}")
                return

            # C. 🌟 核心玩法修正：动能衰减平仓（必须满足盈利门槛！）
            # 只有当当前的利润已经【铁定覆盖双边手续费与滑点】，并且有富余时，才允许看动能信号脸色出场
            if self.holding_tick_counter >= self.min_holding_ticks and return_pct >= self.min_profit_threshold:
                # 利润足够了，此时如果发现动能反转（比如OFI掉头），立刻锁住利润闪电平仓
                if (self.position == 1 and future_ofi_1s < -25.0) or (self.position == -1 and future_ofi_1s > 25.0):
                    self.current_pnl += (return_pct - self.min_friction_pct)
                    self.position = 0
                    self.cooldown_timer = self.cooldown_steps
                    self.pnl.append(self.current_pnl)
                    self.logger.info(f"{dt} [LIVE MOMENTUM CLOSE] price:{return_pct - self.min_friction_pct} | pnl:{self.current_pnl}")
                    self.logger.info(f"----future_ofi_1s:{future_ofi_1s} | future_obi_l5:{future_obi_l5}")
                    self.logger.info(f"----spot_ofi_2s:{spot_ofi_2s} | spot_obi_l5:{spot_obi_l5}")
                    self.logger.info(f"----oi_momentum:{oi_momentum} | future_spot_basis:{future_spot_basis}")
                    self.logger.info(f"----premium_discount: {premium_discount}")
                    return

        # 3. 如果没有持仓且不在冷静期，检查开仓信号
        elif self.cooldown_timer == 0:
            # 检查盘口摩擦，如果当前盘口拉得很稀（Spread太大），放弃本次冲锋
            current_spread_bps = spread_future / ask_price_future
            if current_spread_bps > 0.0003: # 价差大于万3，不划算
                return

            # 🌟 计算动态冲击成本滑点墙 (Impact Slippage Cushion)
            # 如果特征里包含真实的冲击成本特征（buy_impact_bps），则将其转换为百分比；
            # 若无，则使用当前价差的 1.5 倍作为动态高频 Taker 撞单滑点预估垫片。
            estimated_slippage_pct = buy_impact_bps / 10000.0 if buy_impact_bps is not None else current_spread_bps * 1.5

            if sig_long == 1:
                # Taker 买入：吃掉对方的卖一价 (Ask Price)
                self.position = 1
                self.entry_price = ask_price_future * (1.0 + estimated_slippage_pct)
                self.logger.info(f"{dt} [Taker Open Buy] price:{ask_price_future}")
                self.logger.info(f"----future_ofi_1s:{future_ofi_1s} | future_obi_l5:{future_obi_l5}")
                self.logger.info(f"----spot_ofi_2s:{spot_ofi_2s} | spot_obi_l5:{spot_obi_l5}")
                self.logger.info(f"----oi_momentum:{oi_momentum} | future_spot_basis:{future_spot_basis}")
                self.logger.info(f"----premium_discount: {premium_discount}")
            elif sig_short == -1:
                # Taker 卖出：吃掉对方的买一价 (Bid Price)
                self.position = -1
                self.entry_price = bid_price_future * (1.0 - estimated_slippage_pct)
                self.logger.info(f"{dt} [Taker Open Sell] price:{bid_price_future}")
                self.logger.info(f"----future_ofi_1s:{future_ofi_1s} | future_obi_l5:{future_obi_l5}")
                self.logger.info(f"----spot_ofi_2s:{spot_ofi_2s} | spot_obi_l5:{spot_obi_l5}")
                self.logger.info(f"----oi_momentum:{oi_momentum} | future_spot_basis:{future_spot_basis}")
                self.logger.info(f"----premium_discount: {premium_discount}")

        # 动态更新日内最高净值水位线，用于防回撤熔断
        if self.current_pnl > max_pnl_tracked:
            max_pnl_tracked = self.current_pnl
