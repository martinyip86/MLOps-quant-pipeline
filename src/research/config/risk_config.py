from dataclasses import dataclass

@dataclass(frozen=True)
class RiskConfig:
    # 帳號
    balance:float = 50_000

    # 硬性限制指標
    hard_daily_loss_pct:float = 0.03
    hard_total_drawdown_pct:float = 0.06

    # 內部限制指標
    soft_daily_loss_pct:float = 0.01
    soft_total_drawdown_pct:float = 0.04
    risk_per_trade_pct:float = 0.002

    # 交易成本
    fee_bps_per_side:float = 4.0

    # 策略參數，之後再根據分布調整
    stop_loss_bps:float = 6.0
    take_profit_bps:float = 8.0
    max_hold_minutes:int = 15

    @property
    def hard_daily_loss_usdt(self) -> float:
        return self.balance * self.hard_daily_loss_pct

    @property
    def hard_total_drawdown_usdt(self) -> float:
        return self.balance * self.hard_total_drawdown_pct

    @property
    def soft_daily_loss_usdt(self) -> float:
        return self.balance * self.soft_daily_loss_pct

    @property
    def soft_total_drawdown_usdt(self) -> float:
        return self.balance * self.soft_total_drawdown_pct

    @property
    def risk_per_trade_usdt(self) -> float:
        return self.balance * self.risk_per_trade_pct

    @property
    def max_hold_ms(self) -> int:
        return self.max_hold_minutes * 60_000