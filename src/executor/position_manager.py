from dataclasses import dataclass
from typing import Optional
import time

from src.executor.feature_state import FeatureState

@dataclass
class CloseDecision:
    should_close:bool
    reason:str
    exit_price:Optional[float] = None
    pnl_usd:float = 0.0
    pnl_bps:float = 0.0

class PositionManager:
    def __init__(self):
        self.stop_loss_bps = -25.0
        self.take_profit_bps = 30.0
        self.max_hold_ms = 50_000
        self.cooldown_after_close_ms = 3_000

        self.taker_fee_bps = 5.0
        self.slippage_bps = 3.0

        self.reverse_future_obi_threshold = 0.30
        self.reverse_ofi_threshold = -5.0

    def check_exit(self,symbol:str,state:FeatureState) -> CloseDecision:
        position = state.get_position(symbol)
        snapshot = state.get_snapshot(symbol)
        features = state.get_features(symbol)

        if position["side"] is None:
            return CloseDecision(False,"no position")
        
        future_ob = snapshot.get("future_orderbook")
        if not future_ob:
            return CloseDecision(False,"no future orderbook")
        
        if position["side"] == "long":
            return self._check_long_exit(position,future_ob,features)
        
        return CloseDecision(False,f"unsupported side: {position['side']}")
        
    def _check_long_exit(self,position:dict,future_ob:dict,features:dict) -> CloseDecision:
        now_ms = int(time.time() * 1000)

        entry_price = position["entry_price"]
        qty = position['qty']
        entry_time = position["entry_time"]

        exit_price = future_ob["bid_price"]
        exit_notional_usd = exit_price * qty

        gross_pnl_usd = (exit_price - entry_price) * qty
        gross_pnl_bps = (exit_price - entry_price) / entry_price * 10_000

        open_fee_usd = position.get("open_fee_usd",0.0)
        close_fee_usd = exit_notional_usd * self.taker_fee_bps / 10_000

        entry_notional_usd = position.get("entry_notional_usd",entry_price * qty)
        slippage_usd = entry_notional_usd * self.slippage_bps * 2 / 10_000
        
        net_pnl_usd = gross_pnl_usd - open_fee_usd - close_fee_usd - slippage_usd
        net_pnl_bps = net_pnl_usd / entry_notional_usd * 10_000

        position["unrealized_pnl"] = net_pnl_usd

        future_obi = features.get("future_obi_l1")
        future_ofi = features.get("future_ob_ofi_1s")

        signal_reverse = (
            future_obi is not None
            and future_ofi is not None
            and (
                future_obi < self.reverse_future_obi_threshold
                or future_ofi < self.reverse_ofi_threshold
            )
        )

        if net_pnl_bps <= self.stop_loss_bps:
            return CloseDecision(True,"stop_loss",exit_price,net_pnl_usd,net_pnl_bps)
        
        if net_pnl_bps >= self.take_profit_bps:
            return CloseDecision(True,"take_profit",exit_price,net_pnl_usd,net_pnl_bps)
        
        # if signal_reverse:
        #     return CloseDecision(True, "signal_reverse", exit_price, net_pnl_usd, net_pnl_bps)
        
        if now_ms - entry_time >= self.max_hold_ms:
            return CloseDecision(True,"max_hold_timeout",exit_price,net_pnl_usd,net_pnl_bps)
        
        return CloseDecision(False,"hold",exit_price,net_pnl_usd,net_pnl_bps)
    
    def close_position(self,symbol:str,state:FeatureState,close_decision:CloseDecision):
        position = state.get_position(symbol)
        risk = state.get_risk(symbol)

        now_ms = int(time.time() * 1000)

        risk["daily_pnl"] += close_decision.pnl_usd

        if close_decision.reason == "stop_loss":
            risk["stop_loss_count"] += 1
            risk["last_stop_loss_time"] = now_ms

        risk["cooldown_until"] = now_ms + self.cooldown_after_close_ms

        position["side"] = None
        position["qty"] = 0.0
        position["entry_price"] = None
        position["open_fee_usd"] = 0.0
        position["entry_notional_usd"] = 0.0
        position["entry_time"] = None
        position["unrealized_pnl"] = 0.0

        return {
            "symbol":symbol,
            "reason":close_decision.reason,
            "exit_price":close_decision.exit_price,
            "pnl_usd":close_decision.pnl_usd,
            "pnl_bps":close_decision.pnl_bps,
            "daily_pnl":risk["daily_pnl"]
        }