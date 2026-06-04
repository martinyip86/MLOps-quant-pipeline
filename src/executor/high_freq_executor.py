import polars as pl
import argparse
import asyncio
import json
from collections import deque

from src.storage.redis.client import redis_manager
from src.utils.logger import setup_logger
from src.analytics.generate_features import generate_maker_features
from src.core.events import Signal, SignalSide
from src.portfolio.allocator import PortFolioAllocator
from src.risk.risk_engine import RiskEngine
from src.executor.order_manager import OrderManager
from src.executor.paper_executor import PaperExecutor
from src.state.position_manager import PositionManager


class SimpleRegimeStateMachineStrategy:
    def __init__(
            self,
            strategy_id:str,
            symbol:str,
            ofi_threshold:float=30.0,
            obi_threshold:float=0.75,
            max_spread_pct:float=0.0003
        ):
        self.strategy_id = strategy_id
        self.symbol = symbol
        self.ofi_threshold = ofi_threshold
        self.obi_threshold = obi_threshold
        self.max_spread_pct = max_spread_pct
        self.last_regime = "UNKNOWN"
        self.last_state = "FLAT"

    def on_features(self,row:dict,current_position:float) -> Signal:
        timestamp = int(row.get("timestamp",0))
        state = self._position_to_state(current_position)
        regime = self._classify_regime(row)
        self.last_state = state
        self.last_regime = regime

        if regime == "RISK_OFF":
            side = SignalSide.EXIT if state != "FLAT" else SignalSide.HOLD
        elif state == "FLAT" and regime == "BULL":
            side = SignalSide.LONG
        elif state == "FLAT" and regime == "BEAR":
            side = SignalSide.SHORT
        elif state == "LONG" and regime == "BEAR":
            side = SignalSide.EXIT
        elif state == "SHORT" and regime == "BULL":
            side = SignalSide.EXIT
        else:
            side = SignalSide.HOLD

        strength = 1.0 if side != SignalSide.HOLD else 0.0
        confidence = 0.8 if side != SignalSide.HOLD else 0.0
        reason = self._build_reason(row,state,regime)

        return Signal(
            strategy_id=self.strategy_id,
            symbol=self.symbol,
            side=side,
            strength=strength,
            confidence=confidence,
            reason=reason,
            timestamp=timestamp
        )

    def _position_to_state(self,current_position:float) -> str:
        if current_position > 0:
            return "LONG"

        if current_position < 0:
            return "SHORT"

        return "FLAT"

    def _classify_regime(self,row:dict) -> str:
        ask_price = self._to_float(row.get("ask_price_future"))
        spread = self._to_float(row.get("spread_future"))
        future_ofi_1s = self._to_float(row.get("future_ofi_1s"))
        future_obi_l5 = self._to_float(row.get("future_obi_l5"))

        if ask_price <= 0:
            return "RISK_OFF"

        spread_pct = spread / ask_price

        if spread_pct > self.max_spread_pct:
            return "RISK_OFF"

        if future_ofi_1s >= self.ofi_threshold and future_obi_l5 >= self.obi_threshold:
            return "BULL"

        if future_ofi_1s <= -self.ofi_threshold and future_obi_l5 <= -self.obi_threshold:
            return "BEAR"

        return "NEUTRAL"

    def _build_reason(self,row:dict,state:str,regime:str) -> str:
        return (
            f"state={state},regime={regime},"
            f"future_ofi_1s={self._to_float(row.get('future_ofi_1s')):.2f},"
            f"future_obi_l5={self._to_float(row.get('future_obi_l5')):.4f},"
            f"spread_future={self._to_float(row.get('spread_future')):.4f}"
        )

    def _to_float(self,value) -> float:
        if value is None:
            return 0.0

        return float(value)


class HighFreqTakerStrategyExecutor:
    def __init__(self,symbols:list[str],base_qty:float=0.001,min_required_samples:int=100):
        self.redis = redis_manager.connect
        self.symbols = symbols
        self.exchange_id = 'binance'
        self.min_required_samples = min_required_samples

        self.logger = setup_logger(
            name='high_freq_taker_strategy',
            log_file='logs/executor/high_freq_taker_strategy.log'
        )

        # 🌟 核心防线：Executor 专属消费者组，确保与 Syncer 互不干扰，独自享用全量广播流
        self.group_name = 'executor_strategy_group'
        self.consumer_name = 'executor_instance_01'

        # 动态跟踪我们关心的 Streams
        self.streaming_keys = {}

        # 1. 为每种数据流定义内存队列（保持最近 5-10 秒的数据就足够满足你 rolling_sum(50) 的需求了）
        # 假设 100ms 一个点，50个点是 5秒，存 200 个点（20秒）足够安全
        self.buffers = {
            symbol:{
                'orderbook_spot': deque(maxlen=200),
                'orderbook_future': deque(maxlen=200),
                'trades_spot': deque(maxlen=1000),      # 成交可能密集，容量给大点
                'trades_future': deque(maxlen=1000),
                'mark_price_future': deque(maxlen=200),
                'open_interest_future': deque(maxlen=200)
            } for symbol in symbols
        }

        self.strategies = {
            symbol: SimpleRegimeStateMachineStrategy(
                strategy_id="simple_regime_state_machine_v1",
                symbol=symbol
            ) for symbol in symbols
        }
        self.allocator = PortFolioAllocator(base_qty=base_qty)
        self.risk_engine = RiskEngine(
            max_abs_position=base_qty,
            max_order_qty=base_qty * 2,
        )
        self.position_manager = PositionManager()
        self.order_manager = OrderManager()
        self.paper_executor = PaperExecutor(self.position_manager)

    async def _get_redis_streaming_key(self):
        while True:
            for w_type in ['orderbook','trades','market_price','open_interest']:
                registry = f"registry:streams:{w_type}"
                remote_keys = await self.redis.smembers(registry)
                for remote_key in remote_keys:
                    if remote_key not in self.streaming_keys:
                        try:
                            await self.redis.xgroup_create(
                                name=remote_key,
                                groupname=self.group_name,
                                id='0',
                                mkstream=True
                            )
                            self.logger.info(f"✅ Created group {self.group_name} for {remote_key}")
                            self.streaming_keys[remote_key] = ">"
                        except Exception as e:
                            if "BUSYGROUP" in str(e):
                                self.streaming_keys[remote_key] = ">"
                            else:
                                self.logger.error(f"❌ [REGISTRY-ERROR] {e}")
                                await asyncio.sleep(5)

                await asyncio.sleep(30)

    async def _distribute_data(self):
        pending_ack = {}
        while True:
            if not self.streaming_keys:
                await asyncio.sleep(1)
                continue

            response = await self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumer_name,
                streams=self.streaming_keys,
                count=500,
                block=2000
            )

            if response:
                for stream_name,message in response:
                    if stream_name not in pending_ack:
                        pending_ack[stream_name] = []

                    parts = stream_name.split(':')
                    mkt_type = parts[-3]
                    symbol = parts[-2].replace('-','/')
                    data_type = parts[-1]
                    buffer_key = self._to_buffer_key(data_type, mkt_type)

                    for msg_id,content in message:
                        pending_ack[stream_name].append(msg_id)
                        if symbol not in self.buffers or buffer_key not in self.buffers[symbol]:
                            self.logger.debug(f"Skip unsupported stream: {stream_name}")
                            await self.redis.xack(stream_name, self.group_name, msg_id)
                            continue

                        self.buffers[symbol][buffer_key].append(json.loads(content['data']))
                        await self.redis.xack(stream_name, self.group_name, msg_id)

    def _to_buffer_key(self,data_type:str,mkt_type:str) -> str:
        if data_type == "market_price":
            data_type = "mark_price"

        return f"{data_type}_{mkt_type}"

    def _build_feature_row(self,symbol:str) -> dict:
        buffers = self.buffers[symbol]

        df_orderbook_spot = pl.DataFrame(list(buffers['orderbook_spot'])).lazy()
        df_orderbook_future = pl.DataFrame(list(buffers['orderbook_future'])).lazy()
        df_trades_spot = pl.DataFrame(list(buffers['trades_spot'])).lazy()
        df_trades_future = pl.DataFrame(list(buffers['trades_future'])).lazy()
        df_mark_price_future = pl.DataFrame(list(buffers['mark_price_future'])).lazy()
        df_open_interest_future = pl.DataFrame(list(buffers['open_interest_future'])).lazy()

        df = generate_maker_features(
            df_orderbook_spot=df_orderbook_spot,
            df_orderbook_future=df_orderbook_future,
            df_trades_spot=df_trades_spot,
            df_trades_future=df_trades_future,
            df_mark_price=df_mark_price_future,
            df_open_interest=df_open_interest_future
        )

        return df.collect().tail(1).row(0,named=True)

    async def _run_execution_pipeline(self,symbol:str,row:dict):
        current_position = self.position_manager.get_qty(symbol)
        signal = self.strategies[symbol].on_features(row,current_position)
        targets = self.allocator.allocate([signal])

        if not targets:
            return

        for target in targets:
            current_position = self.position_manager.get_qty(target.symbol)
            risk_ok, risk_reason = self.risk_engine.check_target(target,current_position)

            if not risk_ok:
                self.logger.warning(
                    f"RiskCheck rejected target | symbol={target.symbol} "
                    f"current={current_position} target={target.target_qty} reason={risk_reason}"
                )
                continue

            order = self.order_manager.create_order(target,self.position_manager)

            if order is None:
                self.logger.debug(
                    f"No OrderIntent needed | symbol={target.symbol} position={current_position}"
                )
                continue

            fill = await self.paper_executor.execute(order)
            new_position = self.position_manager.on_fill(fill)
            self.logger.info(
                "Signal -> TargetPosition -> RiskCheck -> OrderIntent -> Executor -> Fill -> Position | "
                f"signal={signal.side.value} target={target.target_qty} "
                f"order={order.side} {order.qty} fill={fill.status} position={new_position} "
                f"reason={target.reason}"
            )

    async def executor(self):
        while True:
            await asyncio.sleep(0.1)
            # 确保 6 个队列都有基本数据才开始连表
            for symbol in self.symbols:
                buffers = self.buffers[symbol]

                is_ready = all(
                    len(queue) >= self.min_required_samples
                    for queue in buffers.values()
                )

                if not is_ready:
                    # 如果数据不够，打印 debug 日志展示进度，然后跳过该币种
                    current_status = {k:len(v) for k,v in buffers.items()}
                    self.logger.debug(f"⏳ [{symbol}] 数据预热中... 当前进度: {current_status}")
                    continue

                # 🔥 数据已就绪，开始转换为 Polars LazyFrame 并计算特征
                try:
                    row = self._build_feature_row(symbol)
                    await self._run_execution_pipeline(symbol,row)
                except Exception as e:
                    self.logger.error(f"❌ [{symbol}] Polars pipeline 运行报错: {e}", exc_info=True)

    async def main(self):
        tasks = [
            asyncio.create_task(self._get_redis_streaming_key()),
            asyncio.create_task(self._distribute_data()),
            asyncio.create_task(self.executor())
        ]

        await asyncio.gather(*tasks)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Redis stream data -> regime strategy -> paper execution pipeline."
    )
    parser.add_argument(
        "--symbols",
        default="BTC/USDT",
        help="Comma separated symbols, for example: BTC/USDT,ETH/USDT"
    )
    parser.add_argument(
        "--base-qty",
        type=float,
        default=0.001,
        help="Target position size used by the allocator."
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=100,
        help="Minimum samples required in each Redis data buffer before running features."
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    symbols = [symbol.strip() for symbol in args.symbols.split(",") if symbol.strip()]
    executor = HighFreqTakerStrategyExecutor(
        symbols=symbols,
        base_qty=args.base_qty,
        min_required_samples=args.min_samples
    )
    asyncio.run(executor.main())
