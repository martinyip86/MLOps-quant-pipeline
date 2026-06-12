import json
import time

from logging import Logger
from src.strategies.taker_trend_strategy import Signal
from src.executor.feature_state import FeatureState
from src.executor.paper_order_manager import PaperFill

class TradeRecorder:
    def __init__(self,logger:Logger):
        self.logger = logger

    def recorder_open(self,signal:Signal,fill:PaperFill,state:FeatureState):
        features = state.get_features(signal.symbol)

        self.logger.info(
            "[TRADE_OPEN] " + json.dumps(
                {
                    "ts":int(time.time() * 1000),
                    "symbol":signal.symbol,
                    "side":signal.side,
                    "action":signal.action,
                    "price":fill.price,
                    "qty":fill.qty,
                    "notional_usd":fill.notional_usd,
                    "fee_usd":fill.fee_usd,
                    "confidence":signal.confidence,
                    "expected_edge_bps":signal.expected_edge_bps,
                    "cost_bps":signal.cost_bps,
                    "reason":signal.reason,
                    "features":features
                },
                ensure_ascii=False
            )
        )

    def recorder_close(self,close_result:dict,state:FeatureState):
        features = state.get_features(close_result["symbol"])

        self.logger.info(
            "[TRADE_CLOSE] " + json.dumps(
                {
                    "ts":int(time.time() * 1000),
                    "symbol":close_result["symbol"],
                    "reason":close_result["reason"],
                    "exit_price":close_result["exit_price"],
                    "pnl_usd":close_result["pnl_usd"],
                    "pnl_bps":close_result["pnl_bps"],
                    "daily_pnl":close_result["daily_pnl"],
                    "features":features
                },
                ensure_ascii=False
            )
        )