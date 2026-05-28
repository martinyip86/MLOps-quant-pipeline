class PaperExecutor:
    def __init__(self,position_manager):
        self.position_manager = position_manager

    async def executor(self,order):
        self.position_manager.update_fill(
            symbol=order.symbol,
            side=order.side,
            qty=order.qty
        )

        return {
            'symbol':order.symbol,
            'side':order.side,
            'qty':order.qty,
            'status':"FILLED",
            "paper":True,
            "reason":order.reason
        }