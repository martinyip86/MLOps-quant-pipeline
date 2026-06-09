from dataclasses import dataclass
import time

from src.executor.feature_state import FeatureState
from src.strategies.taker_trend_strategy import Signal

@dataclass
class PaperFill:
    symbol:str
    side:str
    action:str
    price:float
    qty:float
    notional_usd:float
    fee_usd:float
    reason:str

class PaperOrderManager:
    def __init__(self):
        self.taker_fee_bps = 5.0

    def executor(self,signal:Signal,state:FeatureState):
        symbol = signal.symbol
        snapshot = state.get_snapshot(symbol)
        position = state.get_position(symbol)

        future_ob = snapshot.get("future_orderbook")
        if not future_ob: return None

        if signal.action == "open" and signal.side == "long":
            return self._open_long(signal,position,future_ob)
        
    def _open_long(self,signal:Signal,position:dict,future_ob:dict):
        price = future_ob["ask_price"]
        qty = signal.notional_usd / price
        fee_usd = signal.notional_usd * self.taker_fee_bps / 10_000

        position["side"] = "long"
        position["qty"] = qty
        position["entry_price"] = price
        position["entry_time"] = int(time.time() * 1000)
        position["unrealized_pnl"] = 0.0

        return PaperFill(
            symbol=signal.symbol
        )