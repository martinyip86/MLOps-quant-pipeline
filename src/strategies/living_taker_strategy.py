from src.strategies.base import StrategyBase
from src.core.events import Signal,SignalSide

class LivingTakerStrategy(StrategyBase):
    def __init__(self,symbol:str):
        super().__init__(
            strategy_id="taker_momentum_v1",
            symbol=symbol
        )

        self.cooldown_ticks = 200
        self.cooldown_timer = 0

        self.max_spread_pct = 0.0003

    def on_features(self, row) -> Signal:
        ts = row['timestamp']

        if self.cooldown_timer > 0:
            self.cooldown_timer -= 1
            return Signal(
                strategy_id=self.strategy_id,
                symbol=self.symbol,
                side=SignalSide.HOLD,
                strength=0.0,
                confidence=0.0,
                reason="cooldown",
                timestamp=ts
            )
        
        ask = row['ask_price_future']
        spread = row['spread_future']
        spread_pct = spread / ask

        if spread_pct > self.max_spread_pct:
            return Signal(
                strategy_id=self.strategy_id,
                symbol=self.symbol,
                side=SignalSide.HOLD,
                strength=0.0,
                confidence=0.0,
                reason="spread_too_wide",
                timestamp=ts
            )
        
        sig_long = row.get('signal_long',0)
        sig_short = row.get('signal_short',0)

        if sig_long == 1:
            return Signal(
                strategy_id=self.strategy_id,
                symbol=self.symbol,
                side=SignalSide.LONG,
                strength=1.0,
                confidence=0.7,
                reason="taker_long_signal",
                timestamp=ts
            )
        
        if sig_short == -1:
            return Signal(
                strategy_id=self.strategy_id,
                symbol=self.symbol,
                side=SignalSide.SHORT,
                strength=1.0,
                confidence=0.7,
                reason="taker_short_signal",
                timestamp=ts
            )
        
        return Signal(
            self.strategy_id,
            self.symbol,
            SignalSide.HOLD,
            0.0,
            0.0,
            "no_signal",
            ts,
        )