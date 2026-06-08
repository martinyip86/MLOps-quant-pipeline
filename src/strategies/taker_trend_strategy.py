import time
from dataclasses import dataclass
from typing import Optional
from src.executor.feature_state import FeatureState

@dataclass
class Signal:
    symbol:str
    side:str        # long | short
    action:str      # open | close
    confidence:float
    reason:str
    notional_usd:float
    expected_edge_bps:float
    cost_bps:float

class TakerTrendStrategy:
    def __init__(self):
        self.name = "taker_trend_v1"

        # 账户本金
        self.equity_usd = 50_000

        # 新手先保守：每次只用 5% 名义本金
        self.position_pct = 0.05

        # taker 手续费，先保守按 5 bps 来算
        self.taker_fee_bps = 5.0

        # 预估滑点，先保守
        self.slippage_bps = 3.0

        # 最低要求：信号优势必须覆盖成本后还有余量
        self.min_net_edge_bps = 3.0

        # 信号阈值，后面可以根据回测调
        self.future_obi_threshold = 0.70
        self.spot_obi_threshold = 0.70
        self.future_ofi_threshold = 0.0
        self.trade_flow_threshold = 0.0

        # 冷却，避免连续乱开
        self.cooldown_ms = 3000
        self.last_signal_ts = {}

    def evaluate(self,symbol:str,state:FeatureState) -> Optional[Signal]:
        now_ms = int(time.time() * 1000)

        last_ts = self.last_signal_ts.get(symbol,0)
        if not - last_ts < self.cooldown_ms: return None

        if not state.is_data_fresh(symbol,max_lag_ms=3000): return None

        f = state.get_features(symbol)
        position = state.get_position(symbol)

        # 已有仓位就不重复开仓
        if position['side'] is not None: return None

        require_keys = [
            "future_obi_l1",
            "spot_obi_l1",
            "future_ob_ofi_1s",
            "future_trade_flow_1s",
            "spread_future",
            "mid_price_future",
        ]

        for k in require_keys:
            if k not in f or f[k] is None:
                return None
            
        # 成本估算：开仓 + 平仓，两边都要付 taker fee
        roundtrip_fee_bps = self.taker_fee_bps * 2
        roundtrip_slippage_bps = self.slippage_bps * 2
        cost_bps = roundtrip_fee_bps + roundtrip_slippage_bps

        # 简单估算信号 edge，先不要太复杂
        expected_edge_bps = self._estimate_edge_bps(f)

        net_edge_bps = expected_edge_bps - cost_bps

        if net_edge_bps < self.min_net_edge_bps: return None

        # 多头趋势条件
        long_condition = (
            f["future_obi_l1"] > self.future_obi_threshold
            and f["spot_obi_l1"] > self.spot_obi_threshold
            and f["future_ob_ofi_1s"] > self.future_ofi_threshold
            and f["future_trade_flow_1s"] > self.trade_flow_threshold
        )

        if not long_condition: return None

        notional_usd = self.equity_usd * self.position_pct

        self.last_signal_ts[symbol] = now_ms

        return Signal(
            symbol=symbol,
            side="long",
            action="open",
            confidence=min(1.0,net_edge_bps / 20),
            reason=(
                f"long trend: future_obi={f['future_obi_l1']:.3f}, "
                f"spot_obi={f['spot_obi_l1']:.3f}, "
                f"future_ofi={f['future_ob_ofi_1s']:.2f}, "
                f"trade_flow={f['future_trade_flow_1s']:.2f}"
            ),
            notional_usd=notional_usd,
            expected_edge_bps=expected_edge_bps,
            cost_bps=cost_bps
        )

    def _estimate_edge_bps(self,f:dict) -> float:
        """
        先用非常保守的规则估算。
        后面应该替换成你回测出来的 bucket edge。
        """
        edge = 0.0

        if f["future_obi_l1"] > 0.70:
            edge += 8.0

        if f["future_obi_l1"] > 0.85:
            edge += 5.0

        if f["spot_obi_l1"] > 0.70:
            edge += 4.0

        if f["future_ob_ofi_1s"] > 0:
            edge += 3.0

        if f["future_trade_flow_1s"] > 0:
            edge += 3.0

        return edge