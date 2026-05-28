from abc import ABC,abstractmethod
from src.core.events import Signal

class StrategyBase(ABC):
    def __init__(self,strategy_id:str,symbol:str):
        self.strategy_id = strategy_id
        self.symbol = symbol

    @abstractmethod
    def on_features(self,row:dict) -> Signal:
        pass