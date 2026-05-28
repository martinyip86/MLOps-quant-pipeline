from src.core.events import OrderIntent

class OrderManager:
    def __init__(self,targets,position_manager):
        orders = []

        for target in targets:
            current_qty = position_manager.get_qty(target.symbol)
            diff = target.target_qty = current_qty

            if abs(diff) < 1e-9:
                continue

            if diff > 0:
                side = "BUY"
                qty = diff
            else:
                side = "SELL"
                qty = abs(diff)

            reduce_only = (current_qty > 0 and side == "SELL") or (current_qty < 0 and side == "BUY")

            orders.append(
                OrderIntent(
                    symbol=target.symbol,
                    side=side,
                    qty=qty,
                    order_type="MARKET",
                    reduce_only=reduce_only,
                    reason=target.reason
                )
            )

        return orders