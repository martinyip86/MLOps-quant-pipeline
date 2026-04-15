from dataclasses import dataclass
from collections import deque
from typing import Dict
from src.analytics.indicators import calc_vamp_expr,calc_ofi_expr
from src.utils.weight_manager import WeightManager
import polars as pl
import numpy as np

@dataclass
class AlphaSignal:
    timestamp:int
    symbol:str
    long_signal:float
    short_signal:float
    long_strength:float
    short_strength:float
    recommendation:str
    confidence:float
    metadata:Dict

class AlphaModel:
    def __init__(self,exchange_id:str,mkt_type:str,symbol:str,watch_type:str):  
        self.exchange_id = exchange_id
        self.mkt_type = mkt_type
        self.symbol = symbol.replace('/','-')
        self.watch_type = watch_type
        self.weight_manager = WeightManager()
        self.config = self.weight_manager.load_weight(exchange_id, mkt_type, symbol,watch_type)
        if not self.config:
            raise RuntimeError(f"权重文件未找到: {symbol}")
        
        self.long_cfg = self.config['long']
        self.short_cfg = self.config['short']

        self.history = deque(maxlen=50)

        print(f"✅ [AlphaModel] 加载成功 → {symbol} | Long Threshold: {self.long_cfg['threshold']:.4f}")

    def generate_signal(self,tick: dict):
        self.history.append(tick)

        if len(self.history) < 25:
            return None

        df = pl.DataFrame(self.history)
        
        df = df.with_columns([
            calc_vamp_expr(depth=5),
            calc_ofi_expr(window=20)
        ]).with_columns([
            ((pl.col('vamp') - pl.col('ask_prices').list.get(0)) / pl.col('ask_prices').list.get(0) * 10000).alias('vamp_bias_long'),
            ((pl.col('bid_prices').list.get(0) - pl.col('vamp')) / pl.col('bid_prices').list.get(0) * 10000).alias('vamp_bias_short'),
            (pl.col('bid_volumes').list.get(0) / (pl.col('bid_volumes').list.get(0) + pl.col('ask_volumes').list.get(0) + 1e-8)).alias('imbalance')
        ])

        latest = df.tail(1)

        vamp_long = latest['vamp_bias_long'][0]
        vamp_short = latest['vamp_bias_short'][0]
        ofi = latest['factor_ofi_smooth'][0]
        imb = latest['imbalance'][0]
        
        long_raw = (self.long_cfg['w_vamp'] * vamp_long + self.long_cfg['w_ofi'] * ofi + self.long_cfg['w_imbalance'] * imb + self.long_cfg['intercept'])
    
        short_raw = (self.short_cfg['w_vamp'] * vamp_short + self.short_cfg['w_ofi'] * ofi + self.short_cfg['w_imbalance'] * imb + self.short_cfg['intercept'])

        long_strength = long_raw / self.long_cfg['signal_scale']
        short_strength = short_raw / self.short_cfg['signal_scale']

        best_bid = tick['bid_prices'][0]
        best_ask = tick['ask_prices'][0]
        current_spread_bps = (best_ask / best_bid - 1) * 10000

        recommendation = "NEUTRAL"
        confidence = 0.0

        if current_spread_bps < 5.0:
            if long_strength > self.long_cfg['threshold']:
                recommendation = "LONG"
                confidence = float(long_strength)
            elif short_strength > self.short_cfg['threshold']:
                recommendation = "SHORT"
                confidence = float(short_strength)

        return AlphaSignal(
            timestamp=int(tick["timestamp"]),
            symbol=self.symbol,
            long_signal=float(long_raw),
            short_signal=float(short_raw),
            long_strength=float(long_strength),
            short_strength=float(short_strength),
            recommendation=recommendation,
            confidence=confidence,
            metadata={
                "spread_bps": current_spread_bps,
                "best_bid":best_bid,
                "best_ask":best_ask
            }
        )