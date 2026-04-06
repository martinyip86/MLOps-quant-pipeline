from src.analytics.indicators import cal_ofi_expr,cal_vamp_expr
from src.utils.weight_manager import WeightManager
from datetime import datetime,timezone
import polars as pl
import numpy as np


class AlphaModel:
    def __init__(self,exchange_id:str,mkt_type:str,symbol:str,watch_type:str,date_str=None):
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime('%Y%m%d')
        
        self.exchange_id = exchange_id
        self.mkt_type = mkt_type
        self.symbol = symbol
        self.watch_type = watch_type

    def generate_signal(self,df:pl.DataFrame):
        weight_model = WeightManager()
        config = weight_model.load_weight(self.exchange_id,self.mkt_type,self.symbol,self.watch_type)
        


    