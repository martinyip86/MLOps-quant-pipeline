import time
from datetime import datetime,timezone

class FeatureState:
    def __init__(self,symbols:list[str]):
        self.state = {
            symbol:{
                "features":{},
                "snapshot":{},
                "position":{
                    "side":None,
                    "qty":0.0,
                    "open_fee_usd":0.0,
                    "entry_notional_usd":0.0,
                    "entry_price":None,
                    "entry_time":None,
                    "unrealized_pnl":0.0,
                },
                "risk":{
                    "cooldown_until":0,
                    "stop_loss_count":0,
                    "last_stop_loss_time":None,
                    "daily_pnl":0.0,
                    "can_trade":True,
                    "risk_date":None,
                },
                "meta":{
                    "update_at":0,
                    "feature_ts":None,
                    "spot_orderbook_ts":None,
                    "future_orderbook_ts":None,
                }
            }
            for symbol in symbols
        }

    def update_market(self,symbol:str,features:dict,snapshot:dict):
        s = self.state[symbol]

        s["features"].update(features)
        s["snapshot"] = snapshot

        s["meta"]["update_at"] = int(time.time() * 1000)

        if snapshot.get("spot_orderbook"):
            s["meta"]["spot_orderbook_ts"] = snapshot["spot_orderbook"]["timestamp"]

        if snapshot.get("future_orderbook"):
            s["meta"]["future_orderbook_ts"] = snapshot["future_orderbook"]["timestamp"]

        s["meta"]["feature_ts"] = max(
            ts for ts in [s["meta"]["spot_orderbook_ts"],s["meta"]["future_orderbook_ts"]]
            if ts is not None
        )

    def reset_daily_risk_if_needed(self,symbol:str):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        risk = self.state[symbol]["risk"]

        if risk["risk_date"] != today:
            risk["risk_date"] = today
            risk["daily_pnl"] = 0.0
            risk["stop_loss_count"] = 0
            risk["can_trade"] = True

    def get_features(self,symbol:str) -> dict:
        return self.state[symbol]["features"]
    
    def get_snapshot(self,symbol:str) -> dict:
        return self.state[symbol]["snapshot"]
    
    def get_position(self,symbol:str) -> dict:
        return self.state[symbol]["position"]
    
    def get_risk(self,symbol:str) -> dict:
        return self.state[symbol]["risk"]
    
    def is_data_fresh(self,symbol:str,max_lag_ms:int=3000) -> bool:
        now_ms = int(time.time() * 1000)
        feature_ts = self.state[symbol]["meta"]["feature_ts"]

        if feature_ts is None: return False

        return now_ms - feature_ts <= max_lag_ms
