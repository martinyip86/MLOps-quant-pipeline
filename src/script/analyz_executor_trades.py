import json
import re
from pathlib import Path

LOG_PATH = Path("logs/executor/executor_binance.log")

def extract_json(line:str):
    m = re.search(r"\{.*\}",line)
    if not m: return None
    return json.loads(m.group(0))

def main():
    opens = []
    closes = []

    with LOG_PATH.open("r",encoding="utf-8") as f:
        for line in f:
            if "[TRADE_OPEN]" in line:
                data = extract_json(line)
                if data:
                    opens.append(data)
            elif "[TRADE_CLOSE]" in line:
                data = extract_json(line)
                if data:
                    closes.append(data)

    total = len(closes)
    if total == 0:
        print("No closed trades yet.")
        return