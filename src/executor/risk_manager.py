from dataclasses import dataclass
import time

from src.strategies.taker_trend_strategy import Signal
from src.executor.feature_state import FeatureState

@dataclass
class RiskDecision:
    allowed:bool
    reason:str

class RiskManager:
    def __init__(self):
        self.max_notional_usd = 2_500
        self.max_daily_loss_usd = 500
        self.max_stop_loss_count = 5
        self.max_spread_bps = 3.0
        self.max_data_lag_ms = 3_000

    def check_signal(self,signal:Signal,state:FeatureState) -> RiskDecision:
        symbol = signal.symbol

        if not state.is_data_fresh(symbol,self.max_data_lag_ms):
            return RiskDecision(False,"data not fresh")
        
        risk = state.get_risk(symbol)
        position = state.get_position(symbol)
        features = state.get_features(symbol)

        now_ms = int(time.time() * 1000)

        if not risk.get("can_trade",True):
            return RiskDecision(False,"risk can_trade=False")
        
        if now_ms < risk.get("cooldown_until",0):
            return RiskDecision(False,"in cooldown")
        
        if risk.get("daily_pnl",0.0) <= -self.max_daily_loss_usd:
            return RiskDecision(False,"daily loss limit reached")
        
        if risk.get("stop_loss_count",0) >= self.max_stop_loss_count:
            return RiskDecision(False,"stop loss count limit reached")
        
        if position["side"] is not None and signal.action == "open":
            return RiskDecision(False,"already has position")
        
        if signal.notional_usd > self.max_notional_usd:
            return RiskDecision(False,"notional too large")
        
        mid = features.get("mid_price_future")
        spread = features.get("spread_future")

        if mid is None or spread is None or mid <= 0:
            return RiskDecision(False,"invalid spread/mid")
        
        spread_bps = spread / mid * 10_000

        if spread_bps > self.max_spread_bps:
            return RiskDecision(False,f"spread too wide: {spread_bps:.2f} bps")
        
        if signal.expected_edge_bps <= signal.cost_bps:
            return RiskDecision(False,"edge cannot cover cost")
        
        return RiskDecision(True,"risk passed")