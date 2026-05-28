class PositionManager:
    def __init__(self):
        self.positions = {}

    def get_qty(self,symbol:str) -> float:
        return self.positions.get(symbol,0.0)
    
    def update_fill(self,symbol:str,side:str,qty:float):
        current = self.get_qty(symbol)

        if side == "BUY":
            current += qty
        elif side == 'SHORT':
            current -= qty

        self.positions[symbol] = current