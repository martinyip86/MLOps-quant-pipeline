import polars as pl
import numpy as np
import sys

class TakerTrendExecutor:
    def __init__(
        self,
        long_obi_th=0.95,
        short_obi_th=-0.95,
        min_flow=100_000,
        take_profit_bps=8,
        stop_loss_bps=-6,
        max_hold_ms=5000,
        entry_fee_bps=2.0,
        exit_fee_bps=2.0,
        cooldown_ms=1000,
    ):
        self.position = None
        self.trades:list[dict] = []
        self.last_exit_ts = None

        self.long_obi_th = long_obi_th
        self.short_obi_th = short_obi_th
        self.min_flow = min_flow
        self.take_profit_bps = take_profit_bps
        self.stop_loss_bps = stop_loss_bps
        self.max_hold_ms = max_hold_ms
        self.entry_fee_bps = entry_fee_bps
        self.exit_fee_bps = exit_fee_bps
        self.cooldown_ms = cooldown_ms

        self.future_obi_l1_th = 0.8768793173506705
        self.spot_obi_l1_th = 0.9307950374053251
        self.future_ob_ofi_1s_th = 11.486
        self.future_ob_ofi_2s_th = 20.408
        self.future_trade_flow_1s_th = 70531.1612
        self.future_trade_flow_2s_th = 148789.84949999998
        self.spot_trade_flow_1s_th = 5136.091135500001
        self.spot_trade_flow_2s_th = 12682.5922802

    def _entry_signal(self,row:dict):
        #long
        if (
            row["future_obi_l1"] >= self.future_obi_l1_th 
            and row["spot_obi_l1"] > self.spot_obi_l1_th
            and row["future_ob_ofi_1s"] >= self.future_ob_ofi_1s_th
            and row["future_ob_ofi_2s"] >= self.future_ob_ofi_2s_th
            and row["future_trade_flow_1s"] >= self.future_trade_flow_1s_th 
            and row["future_trade_flow_2s"] >= self.future_trade_flow_2s_th
            and row["spot_trade_flow_1s"] >= self.spot_trade_flow_1s_th
            and row["spot_trade_flow_2s"] >= self.spot_trade_flow_2s_th
        ):
            return "long"
        
        #short
        # if (
        #     row["future_obi_l1"] <= -self.future_obi_l1_th 
        #     and row["spot_obi_l1"] <= -self.spot_obi_l1_th
        #     and row["future_ob_ofi_1s"] <= -self.future_ob_ofi_1s_th
        #     and row["future_ob_ofi_2s"] <= -self.future_ob_ofi_2s_th
        #     and row["future_trade_flow_1s"] <= -self.future_trade_flow_1s_th 
        #     and row["future_trade_flow_2s"] <= -self.future_trade_flow_2s_th
        #     and row["spot_trade_flow_1s"] <= -self.spot_trade_flow_1s_th
        #     and row["spot_trade_flow_2s"] <= -self.spot_trade_flow_2s_th
        # ):
        #     return "short"
        
        return None
    
    def _open_position(self,row,side):
        if side == "long":
            entry_price = (row["mid_price_future"] * (1 + self.entry_fee_bps / 10000))
        else:
            entry_price = (row["best_bid"] * (1 - self.entry_fee_bps / 10000))

        return {
            "side":side,
            "entry_ts":row["timestamp"],
            "entry_price":entry_price,
            "future_obi_l1":row["future_obi_l1"],
            "spot_obi_l1":row["spot_obi_l1"],
            "future_trade_flow_1s":row["future_trade_flow_1s"],
            "future_trade_flow_2s":row["future_trade_flow_2s"],
            "future_ob_ofi_1s":row["future_ob_ofi_1s"],
            "future_ob_ofi_2s":row["future_ob_ofi_2s"],
            "spot_trade_flow_1s":row["spot_trade_flow_1s"],
            "spot_trade_flow_2s":row["spot_trade_flow_2s"],
            "spread_future":row["spread_future"],
        }
    
    def _exit_signal(self,row:dict):
        gross_bps = self._calc_gross_bps(row)
        hold_ms = row["timestamp"] - self.position["entry_ts"]

        if gross_bps >= self.take_profit_bps:
            return "take_profit"
        elif gross_bps <= self.stop_loss_bps:
            return "stop_loss"
        elif hold_ms >= self.max_hold_ms:
            return "max_hold"
        
        return None

    def _calc_gross_bps(self,row:dict):
        side = self.position["side"]
        entry = self.position["entry_price"]

        if side == "long":
            exit_price = row["mid_price_future"] * (1 - self.exit_fee_bps / 10000)
            return (exit_price - entry) / entry * 10000
        
        else:
            exit_price = row["best_ask"] * (1 + self.exit_fee_bps / 10000)
            return (entry - exit_price) / entry * 10000
        
    def _close_position(self,row:dict,reason):
        side = self.position["side"]
        entry = self.position["entry_price"]

        if side == "long":
            exit_price = (row["mid_price_future"] * (1 - self.exit_fee_bps / 10000))
        elif side == "short":
            exit_price = (row["best_ask"] * (1 + self.exit_fee_bps / 10000))

        gross_bps = self._calc_gross_bps(row)
        net_bps = gross_bps
        hold_ms = row["timestamp"] - self.position["entry_ts"]

        self.trades.append({
            "side":side,
            "entry_ts":self.position["entry_ts"],
            "exit_ts":row["timestamp"],
            "hold_ms":hold_ms,
            "entry_price":entry,
            "exit_price":exit_price,
            "gross_bps":gross_bps,
            "net_bps":net_bps,
            "reason":reason,
            "future_obi_l1":self.position["future_obi_l1"],
            "spot_obi_l1":self.position["spot_obi_l1"],
            "future_trade_flow_1s":self.position["future_trade_flow_1s"],
            "future_ob_ofi_1s":self.position["future_ob_ofi_1s"],
            "spread_future":self.position["spread_future"],
        })

        self.last_exit_ts = row["timestamp"]
        self.position = None

    def main(self,df:pl.DataFrame):
        self.position = None
        self.trades = []
        self.last_exit_ts = None

        for row in df.iter_rows(named=True):
            ts = row["timestamp"]
            if self.position is None:
                if self.last_exit_ts is not None and ts - self.last_exit_ts < self.cooldown_ms: continue
                signal = self._entry_signal(row)
                if signal is not None:
                    self.position = self._open_position(row,signal)

            else:
                close_reason = self._exit_signal(row)

                if close_reason is None: continue

                self._close_position(row,close_reason)

        return self.trades