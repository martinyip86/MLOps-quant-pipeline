import json
import re
from pathlib import Path

from src.executor.feature_state import FeatureState
from src.strategies.taker_trend_strategy import TakerTrendStrategy
from src.executor.risk_manager import RiskManager
from src.executor.paper_order_manager import PaperOrderManager
from src.executor.position_manager import PositionManager

LOG_PATH = Path("logs/executor/executor_binance.log.5")

def extract_json(line:str):
    m = re.search(r"\{.*\}",line)
    if not m: return None
    return json.loads(m.group(0))

def main():
    symbols = ["BTC/USDT"]

    state = FeatureState(symbols)
    strategy = TakerTrendStrategy()
    risk = RiskManager()
    paper_order_manager = PaperOrderManager()
    position_manager = PositionManager()

    trades = []

    with LOG_PATH.open("r",encoding="utf-8") as f:
        for line in f:
            if "[FEATURE]" not in line:
                continue

            data = extract_json(line)
            if not data: continue

            symbol = data["symbol"]
            features = data["features"]
            snapshot = data["snapshot"]

            state.update_market(symbol,features,snapshot)
            state.reset_daily_risk_if_needed(symbol)

            close_decision = position_manager.check_exit(symbol,state)

            if close_decision.should_close:
                close_result = position_manager.close_position(symbol,state,close_decision)
                trades.append(close_result)
                continue

            signal = strategy.evaluate(symbol,state)

            if not signal: continue

            risk_decision = risk.check_signal(signal,state)

            if not risk_decision.allowed: continue

            fill = paper_order_manager.execute(signal,state)

    if not trades:
        print("No replay trades.")
        return
    
    pnls = [x["pnl_usd"] for x in trades]
    bps = [x["pnl_bps"] for x in trades]
    wins = [x for x in trades if x["pnl_usd"] > 0]

    reason_count = {}
    for x in trades:
        reason_count[x["reason"]] = reason_count.get(x["reason"],0) + 1

    print("====== REPLAY REPORT ======")
    print(f"closed trades: {len(trades)}")
    print(f"win rate: {len(wins) / len(trades) * 100:.2f}%")
    print(f"total pnl usd: {sum(pnls):.4f}")
    print(f"avg pnl usd: {sum(pnls) / len(trades):.4f}")
    print(f"avg pnl bps: {sum(bps) / len(bps):.4f}")
    print(f"max pnl bps: {max(bps):.4f}")
    print(f"min pnl bps: {min(bps):.4f}")
    print(f"reason count: {reason_count}")

if __name__ == "__main__":
    main()