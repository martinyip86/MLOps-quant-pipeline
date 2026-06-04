from src.core.events import SignalSide,TargetPosition

class PortFolioAllocator:
    def __init__(self,base_qty:float=0.001):
        self.base_qty = base_qty

    def allocate(self,signals):
        targets = []

        by_symbol = {}

        for signal in signals:
            if signal.side == SignalSide.HOLD:
                continue

            by_symbol.setdefault(signal.symbol,[]).append(signal)

        for symbol,symbol_signals in by_symbol.items():
            score = 0.0
            reasons = []

            for s in symbol_signals:
                if s.side == SignalSide.LONG:
                    score += s.strength * s.confidence
                elif s.side == SignalSide.SHORT:
                    score -= s.strength * s.confidence
                elif s.side == SignalSide.EXIT:
                    score = 0.0

                reasons.append(f"{s.strategy_id}:{s.reason}")

            if score > 0.3:
                target_qty = self.base_qty
            elif score < -0.3:
                target_qty = -self.base_qty
            else:
                target_qty = 0.0

            targets.append(
                TargetPosition(
                    symbol=symbol,
                    target_qty=target_qty,
                    reason=" | ".join(reasons),
                    timestamp=max(s.timestamp for s in symbol_signals),
                )
            )

        return targets
                
