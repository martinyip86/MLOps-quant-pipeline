from dataclasses import dataclass
from enum import Enum

class SignalSide(str,Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    EXIT = "EXIT"
    HOLD = "HOLD"

@dataclass
class Signal:
    strategy_id:str
    symbol:str
    side:SignalSide
    strength:float
    confidence:float
    reason:str
    timestamp:int

@dataclass
class TargetPosition:
    symbol:str
    target_qty:float
    reason:str
    timestamp:int

@dataclass
class OrderIntent:
    symbol:str
    side:str            # BUY / SELL
    qty:float
    order_type:str      # MARKET / LIMIT
    reduce_only:bool
    reason:str

@dataclass
class Fill:
    symbol:str
    side:str
    qty:float
    status:str
    paper:bool
    reason:str
