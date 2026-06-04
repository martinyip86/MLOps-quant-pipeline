class PositionManager:
    def __init__(self):
        self.positions = {}

    def get_qty(self,symbol:str) -> float:
        return self.positions.get(symbol,0.0)
    
    def update_fill(self,symbol:str,side:str,qty:float):
        current = self.get_qty(symbol)

        if side == "BUY":
            current += qty
        elif side == "SELL":
            current -= qty

        self.positions[symbol] = current

    def on_fill(self,fill):
        if fill.status != "FILLED":
            return self.get_qty(fill.symbol)

        self.update_fill(
            symbol=fill.symbol,
            side=fill.side,
            qty=fill.qty
        )

        return self.get_qty(fill.symbol)