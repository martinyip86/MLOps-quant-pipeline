from src.storage.redis.client import redis_manager
from src.utils.logger import setup_logger
from src.executor.feature_state import FeatureState
from src.executor.data_manager import DataManager
from src.strategies.taker_trend_strategy import TakerTrendStrategy
from src.executor.risk_manager import RiskManager
from src.executor.paper_order_manager import PaperOrderManager
from src.executor.exchange_order_manager import ExchangeOrderManager
from src.executor.position_manager import PositionManager
from src.executor.trade_recorder import TradeRecorder
from src.executor.feature_recorder import FeatureRecorder

import asyncio
import json
import os

class RealTimeExecutor:
    def __init__(self):
        self.redis = redis_manager.connect
        self.logger = setup_logger(
            name="executor_message",
            log_file="logs/executor/executor_binance.log"
        )

        self.group_name = "executor_data"
        self.consumername = "executor_01"
        self.streamings:dict[str,str] = {}
        self.symbols = ["BTC/USDT"]

        self.state = FeatureState(self.symbols)
        self.data_manager = DataManager(self.symbols)
        self.strategy = TakerTrendStrategy()
        self.risk_manager = RiskManager()
        self.paper_order_manager = PaperOrderManager()
        self.exchange_order_manager = ExchangeOrderManager(self.logger)
        self.execution_mode = os.getenv("EXECUTION_MODE","paper").lower()
        self.position_manager = PositionManager()
        self.trade_recorder = TradeRecorder(self.logger)
        self.feature_recorder = FeatureRecorder(self.logger)

    async def refresh_stream_keys(self):
        while True:
            for data_type in ['orderbook','trades','market_price','open_interest']:
                registry = f"registry:streams:{data_type}"
                remote_keys = await self.redis.smembers(registry)
                if remote_keys:
                    for remote_key in remote_keys:
                        try:
                            if remote_key not in self.streamings:
                                await self.redis.xgroup_create(
                                    name=remote_key,
                                    groupname=self.group_name,
                                    id="0",
                                    mkstream=True
                                )
                                self.streamings[remote_key] = ">"
                                self.logger.info(f"✅ Created group {self.group_name} for {remote_key}")
                        except Exception as e:
                            if "BUSYGROUP" in str(e):
                                self.streamings[remote_key] = ">"

            await asyncio.sleep(30)

    async def consume_market_data(self):
        
        while True:
            if not self.streamings:
                await asyncio.sleep(1)
                continue

            response = await self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumername,
                streams=self.streamings,
                count=500,
                block=2000
            )

            if response:
                for stream_name,messages in response:
                    for msg_id,content in messages:
                        data = json.loads(content['data'])
                        result = self.data_manager.main(stream_name,data)
                        await self.redis.xack(stream_name,self.group_name,msg_id)
                        if result is None: continue
                            
                        symbol,features,snapshot = result
                        self.state.update_market(symbol,features,snapshot)

                        self.feature_recorder.record(symbol,features,snapshot)

                        self.state.reset_daily_risk_if_needed(symbol)

                        close_decision = self.position_manager.check_exit(symbol,self.state)

                        if close_decision.should_close:
                            if self.execution_mode == "paper":
                                close_result = self.position_manager.close_position(symbol,self.state,close_decision)

                                self.logger.info(
                                    f"[PAPER_CLOSE][{close_result['symbol']}]"
                                    f"reason={close_result['reason']} | "
                                    f"exit_price={close_result['exit_price']} | "
                                    f"pnl_usd={close_result['pnl_usd']:.4f} | "
                                    f"pnl_bps={close_result['pnl_bps']:.2f} | "
                                    f"daily_pnl={close_result['daily_pnl']:.4f}"
                                )

                                self.trade_recorder.recorder_close(close_result,self.state)

                            else:
                                position = self.state.get_position(symbol)
                                close_order = self.exchange_order_manager.close_long(symbol,position["qty"])

                                self.logger.info(
                                    f"[EXCHANGE_CLOSE][{symbol}] "
                                    f"reason={close_decision.reason} | "
                                    f"qty={position['qty']} | "
                                    f"order={close_order}"
                                )

                                close_result = self.position_manager.close_position(
                                    symbol,
                                    self.state,
                                    close_decision
                                )

                                self.trade_recorder.recorder_close(close_result,self.state)

                            continue

                        signal = self.strategy.evaluate(symbol,self.state)

                        if signal:
                            risk_decision = self.risk_manager.check_signal(signal,self.state)

                            if not risk_decision.allowed:
                                self.logger.info(f"[RISK_REJECT][{signal.symbol}] {signal.side}-{signal.action} | {risk_decision.reason} | {signal.reason}")
                                continue

                            self.logger.info(f"[RISK_PASSED][{signal.symbol}] {signal.side}-{signal.action} | {signal.reason}")

                            self.logger.info(
                                f"[{signal.symbol}][{signal.side}-{signal.action}] "
                                f"confidence: {signal.confidence} | "
                                f"notional_usd: {signal.notional_usd} | "
                                f"expected_edge_bps: {signal.expected_edge_bps} | "
                                f"cost_bps: {signal.cost_bps}"
                            )
                            self.logger.info(f"----{signal.reason}")

                            if self.execution_mode == "paper":
                                fill = self.paper_order_manager.execute(signal,self.state)
                                fill_prefix = "PAPER_FILL"
                            else:
                                # Non-paper mode uses ccxt. It is still protected by
                                # LIVE_TRADING_ENABLED inside ExchangeOrderManager, so
                                # EXECUTION_MODE=live alone only creates dry-run payloads.
                                fill = await self.exchange_order_manager.execute(signal,self.state)
                                fill_prefix = "EXCHANGE_FILL"

                            if fill:
                                self.logger.info(
                                    f"[{fill_prefix}][{fill.symbol}][{fill.side}-{fill.action}] "
                                    f"price={fill.price} | qty={fill.qty} | "
                                    f"notional={fill.notional_usd} | fee={fill.fee_usd} | "
                                    f"----{fill.reason}"
                                )

                                self.trade_recorder.recorder_open(signal,fill,self.state)
                            else:
                                self.logger.info(f"[PAPER_REJECT][{signal.symbol}] no fill generated")

                            

    async def main(self):
        tasks = []
        tasks.append(asyncio.create_task(self.refresh_stream_keys()))
        tasks.append(asyncio.create_task(self.consume_market_data()))
        if self.execution_mode != "paper":
            tasks.append(asyncio.create_task(self.refresh_account_snapshot()))
        await asyncio.gather(*tasks)

    async def refresh_account_snapshot(self):
        # Account reads are useful before live trading: they show whether keys,
        # permissions, futures account, balance, and position APIs are working.
        # This task does NOT place orders. Actual order placement is guarded in
        # ExchangeOrderManager by LIVE_TRADING_ENABLED.
        while True:
            try:
                account_snapshot = await self.exchange_order_manager.fetch_account_snapshot(self.symbols)
                self.state.update_account_snapshot(account_snapshot)
            except Exception as e:
                self.logger.error(f"[ACCOUNT_ERROR] {e}")
            await asyncio.sleep(30)

if __name__ == '__main__':
    obj = RealTimeExecutor()
    try:
        asyncio.run(obj.main())
    finally:
        # Close ccxt aiohttp sessions cleanly when the process exits.
        # In Docker this usually runs during graceful shutdown.
        try:
            asyncio.run(obj.exchange_order_manager.close())
        except Exception:
            pass
