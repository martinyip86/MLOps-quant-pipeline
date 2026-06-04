from src.core.events import OrderIntent,TargetPosition

class RiskEngine:
    def __init__(
            self,
            max_abs_position:float = 0.002,
            max_order_qty:float = 0.001,
            daily_loss_limit:float = -300.0
        ):
        self.max_abs_position = max_abs_position
        self.max_order_qty = max_order_qty
        self.daily_loss_limit = daily_loss_limit
        self.realized_pnl = 0.0
        self.kill_switch = False

    def check_target(self,target:TargetPosition,current_position:float):
        if self.kill_switch:
            return False,"kill_swich_on"

        if self.realized_pnl <= self.daily_loss_limit:
            self.kill_switch = True
            return False,"daily_loss_limit_hit"

        diff = target.target_qty - current_position

        if abs(diff) < 1e-9:
            return True,"ok"

        if abs(diff) > self.max_order_qty:
            return False,"order_qty_too_large"

        if abs(target.target_qty) > self.max_abs_position:
            return False,"position_limit_exceeded"

        return True,"ok"

    def check_order(self,order:OrderIntent,current_position:float):
        if self.kill_switch:
            return False,"kill_swich_on"
        
        if self.realized_pnl <= self.daily_loss_limit:
            self.kill_switch = True
            return False,"daily_loss_limit_hit"
        
        if order.qty > self.max_order_qty:
            return False,"order_qty_too_large"
        
        next_position = current_position

        if order.side == "BUY":
            next_position += order.qty
        elif order.side == "SELL":
            next_position -= order.qty

        if abs(next_position) > self.max_abs_position:
            return False,"position_limit_exceeded"
        
        return True,"ok"