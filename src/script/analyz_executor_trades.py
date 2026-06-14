import json
import re
from datetime import datetime
from pathlib import Path

LOG_PATH = Path("logs/executor/executor_binance.log")

def extract_json(line:str):
    m = re.search(r"\{.*\}",line)
    if not m: return None
    return json.loads(m.group(0))

def parse_time(dt):
    if dt is None: return None

    return int(datetime.strptime(dt,"%Y-%m-%d %H:%M:%S").timestamp() * 1000)

def main(start=None,end=None):
    start_ts = parse_time(start)
    end_ts = parse_time(end)

    opens = []
    closes = []

    with LOG_PATH.open("r",encoding="utf-8") as f:
        for line in f:
            if "[TRADE_OPEN]" in line:
                data = extract_json(line)
                if data:
                    if start_ts and data["ts"] < start_ts: continue
                    if end_ts and data["ts"] > end_ts: continue
                    opens.append(data)
            elif "[TRADE_CLOSE]" in line:
                data = extract_json(line)
                if data:
                    if start_ts and data["ts"] < start_ts: continue
                    if end_ts and data["ts"] > end_ts: continue
                    closes.append(data)

    total = len(closes)
    if total == 0:
        print("No closed trades yet.")
        return
    
    pnls = [x["pnl_usd"] for x in closes]
    pnl_bps = [x["pnl_bps"] for x in closes]

    wins = [x for x in closes if x["pnl_usd"] > 0]
    loss = [x for x in closes if x["pnl_usd"] <= 0]

    reason_count = {}
    for x in closes:
        reason = x["reason"]
        reason_count[reason] = reason_count.get(reason,0) + 1

    print("====== EXECUTOR PAPER TRADE REPORT ======")
    if start_ts or end_ts:
        print(f"ts: {start} ===> {end}")
    print(f"open trades: {len(opens)}")
    print(f"closed trades: {total}")
    print(f"win rate: {len(wins) / total * 100:.2f}")
    print(f"total pnl usd: {sum(pnls):.4f}")
    print(f"avg pnl usd: {sum(pnls) / total:.4f}")
    print(f"avg pnl bps: {sum(pnl_bps) / total:.4f}")
    print(f"max pnl bps: {max(pnl_bps):.4f}")
    print(f"min pnl bps: {min(pnl_bps):.4f}")
    print(f"reason count: {reason_count}")

    print("\nLast 10 closed trades:")
    for x in closes[-10:]:
        print(
            f"{x['symbol']} | {x['reason']} | "
            f"pnl_usd={x['pnl_usd']:.4f} | "
            f"pnl_bps={x['pnl_bps']:.4f} | "
            f"daily_pnl={x['daily_pnl']:.4f}"
        )

if __name__ == "__main__":
    main('2026-06-13 00:00:00')