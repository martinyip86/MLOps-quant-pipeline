from src.core.events import Fill

class PaperExecutor:
    def __init__(self,position_manager):
        self.position_manager = position_manager

    async def execute(self,order):
        return Fill(
            symbol=order.symbol,
            side=order.side,
            qty=order.qty,
            status="FILLED",
            paper=True,
            reason=order.reason
        )
