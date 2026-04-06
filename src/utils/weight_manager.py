import os
import json
from datetime import datetime,timezone

class WeightManager:
    def __init__(self):
        pass

    def save_weight(self,weight_dict,exchange_id:str,mkt_type:str,symbol:str,watch_type:str,date_str=None):
        path = self._get_filepath(exchange_id,mkt_type,symbol,watch_type,date_str)
        os.makedirs(os.path.dirname(path),exist_ok=True)

        with open(path,"w") as f:
            json.dump(weight_dict,f,indent=4)

        return path

    def load_weight(self,exchange_id:str,mkt_type:str,symbol:str,watch_type:str,date_str=None):
        path = self._get_filepath(exchange_id,mkt_type,symbol,watch_type,date_str)

        if not os.path.exists(path):
            print(f"⚠️ 未找到权重文件: {path}")
            return None
        
        with open(path,"r") as f:
            return json.load(f)
        
    @staticmethod
    def _get_filepath(exchange_id:str,mkt_type:str,symbol:str,watch_type:str,date_str=None):
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime('%Y%m%d')
            
        filename = f"{date_str}.json"
        return os.path.join(
            "configs/weights",
            exchange_id,
            mkt_type,
            symbol.replace('/','-'),
            watch_type,
            filename
        )