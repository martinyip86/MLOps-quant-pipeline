import polars as pl
import numpy as np

def calculate_signal(bid_price,bid_volume,ask_price,ask_volume):
    """
    最简单的盘口信号逻辑：
    1. 计算 Micro Price (微价): 考虑了深度的中间价
    2. 计算 Imbalance (不平衡度): 看看是买单多还是卖单多
    """
    # 1. 微价：谁的单子多，价格就更靠近谁（权重价格）
    micro_price = (bid_price * ask_volume + ask_price * bid_volume) / (bid_volume + ask_volume)

    # 2. 订单流不平衡：(买单量 - 卖单量) / 总量
    # 结果在 -1 到 1 之间。正数表示买盘强，负数表示卖盘强。
    imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume)

    return micro_price,imbalance

class SimpleBot:
    def __init__(self,threshold=0.5,fee_rate=0.0004):
        self.threshold = threshold # 信号强度阈值，比如 0.5 才进场
        self.fee_rate = fee_rate

        self.position = 0          # 0: 空仓, 1: 持仓
        self.entry_price = 0       # 买入价格

        self.pnl = 0               # 总盈亏
        self.trades = []           # ✅ 记录每一笔交易

    def on_tick(self,price,signal):
        # 开仓
        # 逻辑：如果买盘非常强 (signal > threshold) 且没持仓，就买
        if signal > self.threshold and self.position == 0:
            self.position = 1
            self.entry_price = price
            print(f"🚀 买入！价格: {price:.2f}")

        # 平仓
        # 逻辑：如果卖盘非常强 (signal < -threshold) 且有持仓，就卖
        elif signal < -self.threshold and self.position > 0:
            fee = price * self.fee_rate
            profit = price - self.entry_price - fee
            self.pnl += profit
            self.trades.append(profit)
            self.position = 0
            print(f"📉 卖出！价格: {price:.2f} | 盈亏: {profit:.2f}")

        return self.pnl
    
    def summary(self):
        total_trades = len(self.trades)

        if total_trades == 0:
            print("没有交易发生")
            return
        
        win_trades = len([t for t in self.trades if t > 0])
        loss_trades = len([t for t in self.trades if t <= 0])

        win_rate = win_trades / loss_trades
        avg_pnl = sum(self.trades) / total_trades
        total_pnl = sum(self.trades)

        print("\n📊 回测结果")
        print("-" * 30)
        print(f"交易次数: {total_trades}")
        print(f"胜率: {win_rate:.2%}")
        print(f"平均每笔收益: {avg_pnl:.2f}")
        print(f"总盈亏: {total_pnl:.2f}")
    
if __name__ == '__main__':
    # 1. 初始化我们的机器人
    bot = SimpleBot(threshold=0.1)

    # 2. 模拟一段盘口数据 (买价, 买量, 卖价, 卖量)
    # 这种数据原本是从 Redis 或者数据库里读的
    market_data = [
        (70000, 10, 70010, 10),
        (70005, 50, 70015, 10),
        (70020, 10, 70030, 10),
        (70025, 80, 70035, 10),
        (70030, 10, 70040, 10),
        (70015, 10, 70025, 60),
        (70000, 10, 70010, 80),
        (69980, 10, 69990, 90),
        (69960, 10, 69970, 100),
    ]

    print("开始监听市场数据...\n" + "-"*30)

    for b_p,b_v,a_p,a_v in market_data:
        # 计算当前的中间价和信号
        mid_price = (b_p + a_p) / 2
        _,signal = calculate_signal(b_p,b_v,a_p,a_v)

        print(f"当前价格: {mid_price} | 信号强度: {signal:.2f}")

        # 让机器人根据信号决策
        pnl = bot.on_tick(mid_price,signal)

    print("-"*30)
    bot.summary()