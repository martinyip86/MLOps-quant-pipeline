from dataclasses import dataclass
from datetime import datetime,timedelta,timezone
import time

from src.executor_v2.state import State
from src.strategies.taker_trend_strategy_v2 import Signal


@dataclass
class RiskDecision:
    allowed:bool
    reason:str


class Risk:
    def __init__(self):
        # 策略自己的单笔上限，不是 Breakout 官方规则。
        self.max_notional_usdt = 2_500

        # 连续止损保护属于策略内部风控，不是 Breakout 官方 breach 条件。
        self.max_stop_loss_count = 5

        # 市场数据与账户数据超过这个时间没有刷新时，禁止继续开仓。
        self.max_spread_bps = 3.0
        self.max_data_lag_ms = 3_000
        self.max_account_lag_ms = 3_000

        # 在 Breakout 官方 equity floor 上额外预留 0.25% 初始账户资金。
        # 这可以吸收手续费、滑点和账户数据延迟，但它不是官方规则。
        self.equity_safety_buffer_pct = 0.0025

        # Breakout 当前杠杆：BTC/ETH 最高 5x，其余币种最高 2x。
        self.major_max_leverage = 5.0
        self.altcoin_max_leverage = 2.0

    def check_account(self,state:State) -> RiskDecision:
        """检查整个 Breakout 账户是否仍允许开新仓。"""
        account = state.get_account()
        account_risk = state.account_risk
        now_ms = int(time.time() * 1000)

        # equity 只有在账户快照持续刷新时才有意义。快照缺失或过期时采用 fail-closed：
        # 不开新仓，但已有仓位仍应允许通过 reduce-only 订单平掉。
        account_updated_at = account.get("updated_at")
        if account_updated_at is None:
            return RiskDecision(False,"account data not ready")

        account_lag_ms = now_ms - int(account_updated_at)
        if account_lag_ms < 0 or account_lag_ms > self.max_account_lag_ms:
            return RiskDecision(False,f"account data stale: {account_lag_ms} ms")

        balance = float(account_risk.get("balance") or 0.0)
        equity = float(account_risk.get("equity") or 0.0)

        if balance <= 0 or equity <= 0:
            return RiskDecision(False,"invalid account balance/equity")

        # Breakout 每天 00:30 UTC 用当时的 balance 重算 daily equity floor。
        # risk_reset_at 和 daily_reference_balance 必须持久化；否则程序在日内重启时，
        # 只能使用重启后的当前 balance 近似当天基准，可能与 Breakout Dashboard 不一致。
        self._reset_daily_equity_floor_if_needed(account_risk,now_ms)

        if not account_risk.get("can_trade",True):
            reason = account_risk.get("halt_reason") or "account trading disabled"
            return RiskDecision(False,reason)

        daily_floor = float(account_risk.get("daily_equity_floor") or 0.0)
        drawdown_floor = float(account_risk.get("max_drawdown_floor") or 0.0)

        if daily_floor <= 0 or drawdown_floor <= 0:
            return RiskDecision(False,"invalid Breakout equity floor")

        # 两条线同时有效；数值较高的那条会更早被触及，因此是当前有效底线。
        effective_floor = max(daily_floor,drawdown_floor)

        # 真正触及官方底线时，将账户总开关永久关闭。Breakout 账户一旦 breach，
        # 即使随后行情反弹也不会恢复交易资格。
        if equity <= effective_floor:
            reason = (
                f"Breakout equity limit breached: equity={equity:.2f}, "
                f"floor={effective_floor:.2f}"
            )
            account_risk["can_trade"] = False
            account_risk["halt_reason"] = reason
            return RiskDecision(False,reason)

        initial_balance = float(account_risk.get("initial_balance") or 0.0)
        safety_buffer = initial_balance * self.equity_safety_buffer_pct
        internal_floor = effective_floor + safety_buffer

        if equity <= internal_floor:
            return RiskDecision(
                False,
                f"equity inside safety buffer: equity={equity:.2f}, "
                f"internal_floor={internal_floor:.2f}"
            )

        return RiskDecision(True,"account risk passed")

    def check_signal(self,signal:Signal,state:State) -> RiskDecision:
        symbol = signal.symbol
        position = state.get_position(symbol)

        # 平仓是在降低账户风险。不要因为 cooldown、行情过期、spread 或 edge
        # 而阻止紧急平仓；实际下单时仍必须使用 reduceOnly=True。
        if signal.action == "close":
            if position["side"] is None:
                return RiskDecision(False,"no position to close")
            return RiskDecision(True,"close signal reduces risk")

        if signal.action != "open":
            return RiskDecision(False,f"unsupported signal action: {signal.action}")

        account_decision = self.check_account(state)
        if not account_decision.allowed:
            return account_decision

        if not state.is_data_fresh(symbol,self.max_data_lag_ms):
            return RiskDecision(False,"data not fresh")

        risk = state.get_risk(symbol)
        account_risk = risk["account"]
        symbol_risk = risk[symbol]
        features = state.get_features(symbol)

        now_ms = int(time.time() * 1000)

        if now_ms < symbol_risk.get("cooldown_until",0):
            return RiskDecision(False,"in cooldown")

        if symbol_risk.get("stop_loss_count",0) >= self.max_stop_loss_count:
            return RiskDecision(False,"stop loss count limit reached")

        if position["side"] is not None:
            return RiskDecision(False,"already has position")

        if signal.notional_usd <= 0:
            return RiskDecision(False,"invalid order notional")

        if signal.notional_usd > self.max_notional_usdt:
            return RiskDecision(False,"notional too large")

        # 用所有品种的绝对 notional 估算占用保证金，避免多品种分别通过检查后，
        # 合计杠杆超过 Breakout 对 BTC/ETH 5x、其他币种 2x 的限制。
        required_margin = self._required_margin_after_signal(state,signal)
        equity = float(account_risk["equity"])
        if required_margin > equity:
            return RiskDecision(
                False,
                f"Breakout leverage limit exceeded: required_margin="
                f"{required_margin:.2f}, equity={equity:.2f}"
            )

        # v2 特征统一使用 swap 命名，而不是旧版本的 future 命名。
        mid = features.get("mid_price_swap")
        spread = features.get("spread_swap")

        if mid is None or spread is None or mid <= 0 or spread < 0:
            return RiskDecision(False,"invalid spread/mid")

        spread_bps = spread / mid * 10_000

        if spread_bps > self.max_spread_bps:
            return RiskDecision(False,f"spread too wide: {spread_bps:.2f} bps")

        if signal.expected_edge_bps <= signal.cost_bps:
            return RiskDecision(False,"edge cannot cover cost")

        # 即使当前 equity 尚未进入安全区，新订单的预估往返成本也不能把账户
        # 推入安全区。价格反向波动的风险仍应由仓位大小和止损逻辑另外控制。
        effective_floor = max(
            float(account_risk["daily_equity_floor"]),
            float(account_risk["max_drawdown_floor"]),
        )
        safety_buffer = (
            float(account_risk["initial_balance"])
            * self.equity_safety_buffer_pct
        )
        estimated_cost = signal.notional_usd * signal.cost_bps / 10_000

        if equity - estimated_cost <= effective_floor + safety_buffer:
            return RiskDecision(False,"insufficient equity headroom after estimated cost")

        return RiskDecision(True,"risk passed")

    def _reset_daily_equity_floor_if_needed(
        self,
        account_risk:dict,
        now_ms:int,
    ) -> None:
        """按 Breakout 的 00:30 UTC 日界线更新当天账户风险底线。"""
        period_start_ms = self._current_breakout_period_start_ms(now_ms)

        if account_risk.get("risk_reset_at") == period_start_ms:
            return

        reference_balance = float(account_risk["balance"])
        loss_limit_pct = float(account_risk["daily_loss_limit_pct"])

        account_risk["daily_reference_balance"] = reference_balance
        account_risk["daily_equity_floor"] = reference_balance * (1 - loss_limit_pct)
        account_risk["risk_reset_at"] = period_start_ms

    @staticmethod
    def _current_breakout_period_start_ms(now_ms:int) -> int:
        """返回当前 Breakout 风控日开始时间，即最近一次 00:30 UTC。"""
        now = datetime.fromtimestamp(now_ms / 1000,tz=timezone.utc)
        period_start = now.replace(hour=0,minute=30,second=0,microsecond=0)

        if now < period_start:
            period_start -= timedelta(days=1)

        return int(period_start.timestamp() * 1000)

    def _required_margin_after_signal(self,state:State,signal:Signal) -> float:
        """按各币种 Breakout 最大杠杆，保守估算开仓后的最低保证金需求。"""
        required_margin = 0.0

        for symbol in state.symbols:
            position = state.get_position(symbol)
            notional = abs(float(position.get("notional") or 0.0))
            required_margin += notional / self._max_leverage(symbol)

        required_margin += signal.notional_usd / self._max_leverage(signal.symbol)
        return required_margin

    def _max_leverage(self,symbol:str) -> float:
        base_asset = symbol.split("/")[0].upper()

        if base_asset in {"BTC","ETH"}:
            return self.major_max_leverage

        return self.altcoin_max_leverage
