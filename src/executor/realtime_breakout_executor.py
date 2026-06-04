import argparse
import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from src.core.events import Fill, OrderIntent, Signal, SignalSide, TargetPosition
from src.executor.paper_executor import PaperExecutor
from src.portfolio.allocator import PortFolioAllocator
from src.risk.risk_engine import RiskEngine
from src.state.position_manager import PositionManager
from src.utils.logger import setup_logger


def now_ms() -> int:
    return int(time.time() * 1000)


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_div(num: float, den: float, default: float = 0.0) -> float:
    if abs(den) < 1e-12:
        return default
    return num / den


@dataclass
class BookState:
    bid_prices: list[float] = field(default_factory=list)
    bid_volumes: list[float] = field(default_factory=list)
    ask_prices: list[float] = field(default_factory=list)
    ask_volumes: list[float] = field(default_factory=list)
    timestamp: int = 0

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "BookState":
        return cls(
            bid_prices=[to_float(v) for v in payload.get("bid_prices", [])],
            bid_volumes=[to_float(v) for v in payload.get("bid_volumes", [])],
            ask_prices=[to_float(v) for v in payload.get("ask_prices", [])],
            ask_volumes=[to_float(v) for v in payload.get("ask_volumes", [])],
            timestamp=int(payload.get("timestamp") or 0),
        )

    @property
    def ready(self) -> bool:
        return bool(self.bid_prices and self.ask_prices and self.bid_volumes and self.ask_volumes)

    @property
    def bid_price(self) -> float:
        return self.bid_prices[0] if self.bid_prices else 0.0

    @property
    def ask_price(self) -> float:
        return self.ask_prices[0] if self.ask_prices else 0.0

    @property
    def mid_price(self) -> float:
        if not self.ready:
            return 0.0
        return (self.bid_price + self.ask_price) / 2.0

    @property
    def spread(self) -> float:
        if not self.ready:
            return 0.0
        return max(0.0, self.ask_price - self.bid_price)

    @property
    def spread_bps(self) -> float:
        return safe_div(self.spread, self.mid_price) * 10000.0

    def obi(self, depth: int) -> float:
        bid = sum(self.bid_volumes[:depth])
        ask = sum(self.ask_volumes[:depth])
        return safe_div(bid - ask, bid + ask)

    def buy_impact_bps(self, qty: float) -> float:
        if qty <= 0 or not self.ready:
            return 0.0

        remaining = qty
        cost = 0.0
        filled = 0.0

        for price, volume in zip(self.ask_prices, self.ask_volumes):
            take = min(remaining, volume)
            cost += take * price
            filled += take
            remaining -= take
            if remaining <= 1e-12:
                break

        if filled <= 0:
            return 0.0

        avg_price = cost / filled
        return safe_div(avg_price - self.ask_price, self.ask_price) * 10000.0


class TradeFlowBuckets:
    def __init__(self, bucket_ms: int = 100, max_window_buckets: int = 50):
        self.bucket_ms = bucket_ms
        self.max_window_buckets = max_window_buckets
        self.current_bucket: int | None = None
        self.current_flow = 0.0
        self.closed_flows: deque[float] = deque(maxlen=max_window_buckets)
        self.last_trade_ts = 0

    def update(self, timestamp: int, signed_amount: float) -> None:
        bucket = timestamp // self.bucket_ms
        self.last_trade_ts = max(self.last_trade_ts, timestamp)

        if self.current_bucket is None:
            self.current_bucket = bucket
            self.current_flow = signed_amount
            return

        if bucket == self.current_bucket:
            self.current_flow += signed_amount
            return

        if bucket < self.current_bucket:
            return

        self.closed_flows.append(self.current_flow)
        for _ in range(min(bucket - self.current_bucket - 1, self.max_window_buckets)):
            self.closed_flows.append(0.0)

        self.current_bucket = bucket
        self.current_flow = signed_amount

    def rolling_sum(self, buckets: int) -> float:
        if buckets <= 1:
            return self.current_flow
        closed_needed = buckets - 1
        return sum(list(self.closed_flows)[-closed_needed:]) + self.current_flow


class OpenInterestTracker:
    def __init__(self, lookback_points: int = 10):
        self.values: deque[tuple[int, float]] = deque(maxlen=lookback_points + 1)

    def update(self, timestamp: int, open_interest_amount: float) -> None:
        self.values.append((timestamp, open_interest_amount))

    @property
    def latest(self) -> float:
        return self.values[-1][1] if self.values else 0.0

    @property
    def timestamp(self) -> int:
        return self.values[-1][0] if self.values else 0

    @property
    def momentum(self) -> float:
        if len(self.values) < 2:
            return 0.0
        return self.values[-1][1] - self.values[0][1]


@dataclass
class RealtimeFeatureSnapshot:
    symbol: str
    timestamp: int
    bid_price_future: float
    ask_price_future: float
    mid_price_future: float
    spread_future: float
    spread_future_bps: float
    buy_impact_bps_future: float
    future_ofi_1s: float
    future_ofi_5s: float
    spot_ofi_1s: float
    spot_ofi_2s: float
    future_obi_l1: float
    future_obi_l3: float
    future_obi_l5: float
    spot_obi_l1: float
    spot_obi_l3: float
    spot_obi_l5: float
    future_spot_basis: float
    future_spot_basis_bps: float
    premium_discount: float
    premium_discount_bps: float
    oi_momentum: float
    mark_price: float

    def as_row(self) -> dict[str, float | int | str]:
        return self.__dict__.copy()


class SymbolRealtimeState:
    def __init__(self, symbol: str, order_qty: float):
        self.symbol = symbol
        self.order_qty = order_qty
        self.spot_book = BookState()
        self.future_book = BookState()
        self.spot_flow = TradeFlowBuckets(bucket_ms=100, max_window_buckets=50)
        self.future_flow = TradeFlowBuckets(bucket_ms=100, max_window_buckets=50)
        self.open_interest = OpenInterestTracker(lookback_points=10)
        self.mark_price = 0.0
        self.mark_price_ts = 0

    def update_orderbook(self, mkt_type: str, payload: dict[str, Any]) -> None:
        book = BookState.from_payload(payload)
        if mkt_type == "spot":
            self.spot_book = book
        elif mkt_type == "future":
            self.future_book = book

    def update_trade(self, mkt_type: str, payload: dict[str, Any]) -> None:
        amount = to_float(payload.get("amount"))
        timestamp = int(payload.get("timestamp") or 0)

        if mkt_type == "spot":
            is_taker_buyer = bool(payload.get("is_taker_buyer"))
            signed_amount = amount if is_taker_buyer else -amount
            self.spot_flow.update(timestamp, signed_amount)
            return

        if mkt_type == "future":
            signed_amount = amount if payload.get("side") == "buy" else -amount
            self.future_flow.update(timestamp, signed_amount)

    def update_market_price(self, payload: dict[str, Any]) -> None:
        self.mark_price = to_float(payload.get("mark_price"))
        self.mark_price_ts = int(payload.get("timestamp") or 0)

    def update_open_interest(self, payload: dict[str, Any]) -> None:
        self.open_interest.update(
            timestamp=int(payload.get("timestamp") or 0),
            open_interest_amount=to_float(payload.get("open_interest_amount")),
        )

    def ready(self, current_ts: int, max_book_age_ms: int, max_mark_age_ms: int) -> tuple[bool, str]:
        if not self.spot_book.ready:
            return False, "missing_spot_book"
        if not self.future_book.ready:
            return False, "missing_future_book"
        if self.mark_price <= 0:
            return False, "missing_mark_price"
        if current_ts - self.spot_book.timestamp > max_book_age_ms:
            return False, "stale_spot_book"
        if current_ts - self.future_book.timestamp > max_book_age_ms:
            return False, "stale_future_book"
        if current_ts - self.mark_price_ts > max_mark_age_ms:
            return False, "stale_mark_price"
        return True, "ok"

    def snapshot(self) -> RealtimeFeatureSnapshot:
        future_mid = self.future_book.mid_price
        spot_mid = self.spot_book.mid_price
        basis = future_mid - spot_mid
        premium = future_mid - self.mark_price

        return RealtimeFeatureSnapshot(
            symbol=self.symbol,
            timestamp=max(self.future_book.timestamp, self.spot_book.timestamp, self.mark_price_ts),
            bid_price_future=self.future_book.bid_price,
            ask_price_future=self.future_book.ask_price,
            mid_price_future=future_mid,
            spread_future=self.future_book.spread,
            spread_future_bps=self.future_book.spread_bps,
            buy_impact_bps_future=self.future_book.buy_impact_bps(self.order_qty),
            future_ofi_1s=self.future_flow.rolling_sum(10),
            future_ofi_5s=self.future_flow.rolling_sum(50),
            spot_ofi_1s=self.spot_flow.rolling_sum(10),
            spot_ofi_2s=self.spot_flow.rolling_sum(20),
            future_obi_l1=self.future_book.obi(1),
            future_obi_l3=self.future_book.obi(3),
            future_obi_l5=self.future_book.obi(5),
            spot_obi_l1=self.spot_book.obi(1),
            spot_obi_l3=self.spot_book.obi(3),
            spot_obi_l5=self.spot_book.obi(5),
            future_spot_basis=basis,
            future_spot_basis_bps=safe_div(basis, spot_mid) * 10000.0,
            premium_discount=premium,
            premium_discount_bps=safe_div(premium, self.mark_price) * 10000.0,
            oi_momentum=self.open_interest.momentum,
            mark_price=self.mark_price,
        )


class CostAwareBreakoutStrategy:
    def __init__(
        self,
        strategy_id: str,
        symbol: str,
        min_future_ofi_1s: float = 30.0,
        min_spot_ofi_2s: float = 15.0,
        min_future_obi_l5: float = 0.75,
        min_spot_obi_l5: float = 0.50,
        max_spread_bps: float = 4.0,
        max_impact_bps: float = 3.0,
        round_trip_fee_bps: float = 8.0,
        safety_margin_bps: float = 6.0,
        min_signal_cost_buffer_bps: float = 11.0,
        confirmation_ticks: int = 3,
        cooldown_ms: int = 60_000,
    ):
        self.strategy_id = strategy_id
        self.symbol = symbol
        self.min_future_ofi_1s = min_future_ofi_1s
        self.min_spot_ofi_2s = min_spot_ofi_2s
        self.min_future_obi_l5 = min_future_obi_l5
        self.min_spot_obi_l5 = min_spot_obi_l5
        self.max_spread_bps = max_spread_bps
        self.max_impact_bps = max_impact_bps
        self.round_trip_fee_bps = round_trip_fee_bps
        self.safety_margin_bps = safety_margin_bps
        self.min_signal_cost_buffer_bps = min_signal_cost_buffer_bps
        self.confirmation_ticks = confirmation_ticks
        self.cooldown_ms = cooldown_ms
        self.long_confirm = 0
        self.short_confirm = 0
        self.last_entry_ts = 0

    def on_features(self, row: dict[str, Any], current_position: float) -> Signal:
        ts = int(row["timestamp"])
        estimated_cost_bps = (
            self.round_trip_fee_bps
            + float(row["spread_future_bps"])
            + float(row["buy_impact_bps_future"]) * 2.0
            + self.safety_margin_bps
        )

        market_is_cheap_enough = (
            row["spread_future_bps"] <= self.max_spread_bps
            and row["buy_impact_bps_future"] <= self.max_impact_bps
        )
        long_raw = (
            row["future_ofi_1s"] >= self.min_future_ofi_1s
            and row["spot_ofi_2s"] >= self.min_spot_ofi_2s
            and row["future_obi_l5"] >= self.min_future_obi_l5
            and row["spot_obi_l5"] >= self.min_spot_obi_l5
            and market_is_cheap_enough
        )
        short_raw = (
            row["future_ofi_1s"] <= -self.min_future_ofi_1s
            and row["spot_ofi_2s"] <= -self.min_spot_ofi_2s
            and row["future_obi_l5"] <= -self.min_future_obi_l5
            and row["spot_obi_l5"] <= -self.min_spot_obi_l5
            and row["oi_momentum"] >= 0.0
            and market_is_cheap_enough
        )

        self.long_confirm = self.long_confirm + 1 if long_raw else 0
        self.short_confirm = self.short_confirm + 1 if short_raw else 0

        side = SignalSide.HOLD
        in_cooldown = ts - self.last_entry_ts < self.cooldown_ms

        if current_position > 0 and short_raw:
            side = SignalSide.EXIT
        elif current_position < 0 and long_raw:
            side = SignalSide.EXIT
        elif current_position == 0 and not in_cooldown:
            if self.long_confirm >= self.confirmation_ticks:
                side = SignalSide.LONG
                self.last_entry_ts = ts
            elif self.short_confirm >= self.confirmation_ticks:
                side = SignalSide.SHORT
                self.last_entry_ts = ts

        strength = 1.0 if side != SignalSide.HOLD else 0.0
        confidence = 0.8 if side != SignalSide.HOLD else 0.0

        reason = (
            f"side={side.value},cost_guard={estimated_cost_bps:.2f}bps,"
            f"f_ofi_1s={row['future_ofi_1s']:.4f},s_ofi_2s={row['spot_ofi_2s']:.4f},"
            f"f_obi_l5={row['future_obi_l5']:.4f},s_obi_l5={row['spot_obi_l5']:.4f},"
            f"spread={row['spread_future_bps']:.2f}bps,impact={row['buy_impact_bps_future']:.2f}bps"
        )

        if estimated_cost_bps < self.round_trip_fee_bps + self.min_signal_cost_buffer_bps:
            reason += ",cost_buffer_ok"
        else:
            reason += ",cost_buffer_watch"

        return Signal(
            strategy_id=self.strategy_id,
            symbol=self.symbol,
            side=side,
            strength=strength,
            confidence=confidence,
            reason=reason,
            timestamp=ts,
        )


class RealtimeBreakoutExecutor:
    def __init__(
        self,
        symbols: list[str],
        base_qty: float = 0.001,
        eval_interval_ms: int = 500,
        max_book_age_ms: int = 1500,
        max_mark_age_ms: int = 10_000,
    ):
        from src.storage.redis.client import redis_manager

        self.redis = redis_manager.connect
        self.symbols = symbols
        self.base_qty = base_qty
        self.eval_interval_ms = eval_interval_ms
        self.max_book_age_ms = max_book_age_ms
        self.max_mark_age_ms = max_mark_age_ms
        self.group_name = "realtime_breakout_executor_group"
        self.consumer_name = "realtime_breakout_executor_01"
        self.streaming_keys: dict[str, str] = {}

        self.logger = setup_logger(
            name="realtime_breakout_executor",
            log_file="logs/executor/realtime_breakout_executor.log",
        )
        self.states = {
            symbol: SymbolRealtimeState(symbol=symbol, order_qty=base_qty)
            for symbol in symbols
        }
        self.strategies = {
            symbol: CostAwareBreakoutStrategy(
                strategy_id="cost_aware_breakout_v1",
                symbol=symbol,
            )
            for symbol in symbols
        }
        self.allocator = PortFolioAllocator(base_qty=base_qty)
        self.risk_engine = RiskEngine(
            max_abs_position=base_qty,
            max_order_qty=base_qty * 2.0,
        )
        self.position_manager = PositionManager()
        self.paper_executor = PaperExecutor(self.position_manager)

    async def refresh_stream_keys(self) -> None:
        while True:
            for data_type in ("orderbook", "trades", "market_price", "open_interest"):
                registry = f"registry:streams:{data_type}"
                remote_keys = await self.redis.smembers(registry)
                for remote_key in remote_keys:
                    if remote_key in self.streaming_keys:
                        continue
                    await self._ensure_group(remote_key)
                    self.streaming_keys[remote_key] = ">"
            await asyncio.sleep(30)

    async def _ensure_group(self, stream_key: str) -> None:
        try:
            await self.redis.xgroup_create(
                name=stream_key,
                groupname=self.group_name,
                id="0",
                mkstream=True,
            )
            self.logger.info("Created Redis group %s for %s", self.group_name, stream_key)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def consume_market_data(self) -> None:
        while True:
            if not self.streaming_keys:
                await asyncio.sleep(1)
                continue

            response = await self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumer_name,
                streams=self.streaming_keys,
                count=500,
                block=2000,
            )

            for stream_name, messages in response or []:
                for msg_id, content in messages:
                    try:
                        self._apply_message(stream_name, content)
                    except Exception as exc:
                        self.logger.warning(
                            "Failed to apply market-data message stream=%s id=%s error=%s",
                            stream_name,
                            msg_id,
                            exc,
                            exc_info=True,
                        )
                    finally:
                        await self.redis.xack(stream_name, self.group_name, msg_id)

    def _apply_message(self, stream_name: str, content: dict[str, Any]) -> None:
        payload = json.loads(content["data"])
        mkt_type, symbol, data_type = self._parse_stream_name(stream_name)
        state = self.states.get(symbol)
        if state is None:
            return

        if data_type == "orderbook":
            state.update_orderbook(mkt_type, payload)
        elif data_type == "trades":
            state.update_trade(mkt_type, payload)
        elif data_type == "market_price":
            state.update_market_price(payload)
        elif data_type == "open_interest":
            state.update_open_interest(payload)

    def _parse_stream_name(self, stream_name: str) -> tuple[str, str, str]:
        parts = stream_name.split(":")
        if len(parts) < 5:
            raise ValueError(f"unsupported stream name: {stream_name}")
        mkt_type = parts[-3]
        symbol = parts[-2].replace("-", "/")
        data_type = parts[-1]
        return mkt_type, symbol, data_type

    async def evaluate_loop(self) -> None:
        sleep_s = self.eval_interval_ms / 1000.0
        while True:
            await asyncio.sleep(sleep_s)
            current_ts = now_ms()

            for symbol, state in self.states.items():
                ready, reason = state.ready(
                    current_ts=current_ts,
                    max_book_age_ms=self.max_book_age_ms,
                    max_mark_age_ms=self.max_mark_age_ms,
                )
                if not ready:
                    self.logger.debug("[%s] waiting for market state: %s", symbol, reason)
                    continue

                snapshot = state.snapshot()
                await self._run_signal_pipeline(snapshot)

    async def _run_signal_pipeline(self, snapshot: RealtimeFeatureSnapshot) -> None:
        row = snapshot.as_row()
        current_position = self.position_manager.get_qty(snapshot.symbol)
        signal = self.strategies[snapshot.symbol].on_features(row, current_position)
        targets = self.allocator.allocate([signal])

        for target in targets:
            await self._execute_target(target)

    async def _execute_target(self, target: TargetPosition) -> None:
        current_position = self.position_manager.get_qty(target.symbol)
        risk_ok, risk_reason = self.risk_engine.check_target(target, current_position)

        if not risk_ok:
            self.logger.warning(
                "Risk rejected target symbol=%s current=%s target=%s reason=%s",
                target.symbol,
                current_position,
                target.target_qty,
                risk_reason,
            )
            return

        order = self._create_order_intent(target, current_position)
        if order is None:
            return

        fill = await self.paper_executor.execute(order)
        new_position = self.position_manager.on_fill(fill)
        self._log_fill(fill, new_position)

    def _create_order_intent(
        self,
        target: TargetPosition,
        current_position: float,
    ) -> OrderIntent | None:
        diff = target.target_qty - current_position
        if abs(diff) < 1e-9:
            return None

        side = "BUY" if diff > 0 else "SELL"
        qty = abs(diff)
        reduce_only = (current_position > 0 and side == "SELL") or (
            current_position < 0 and side == "BUY"
        )

        return OrderIntent(
            symbol=target.symbol,
            side=side,
            qty=qty,
            order_type="MARKET",
            reduce_only=reduce_only,
            reason=target.reason,
        )

    def _log_fill(self, fill: Fill, new_position: float) -> None:
        self.logger.info(
            "paper_fill symbol=%s side=%s qty=%s status=%s position=%s reason=%s",
            fill.symbol,
            fill.side,
            fill.qty,
            fill.status,
            new_position,
            fill.reason,
        )

    async def main(self) -> None:
        await asyncio.gather(
            asyncio.create_task(self.refresh_stream_keys()),
            asyncio.create_task(self.consume_market_data()),
            asyncio.create_task(self.evaluate_loop()),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a low-memory Redis stream -> incremental features -> paper breakout executor."
    )
    parser.add_argument(
        "--symbols",
        default="BTC/USDT",
        help="Comma separated symbols, for example: BTC/USDT,ETH/USDT",
    )
    parser.add_argument(
        "--base-qty",
        type=float,
        default=0.001,
        help="Paper target position size.",
    )
    parser.add_argument(
        "--eval-interval-ms",
        type=int,
        default=500,
        help="How often to evaluate the strategy from the latest in-memory state.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_symbols = [symbol.strip() for symbol in args.symbols.split(",") if symbol.strip()]
    executor = RealtimeBreakoutExecutor(
        symbols=run_symbols,
        base_qty=args.base_qty,
        eval_interval_ms=args.eval_interval_ms,
    )
    asyncio.run(executor.main())
