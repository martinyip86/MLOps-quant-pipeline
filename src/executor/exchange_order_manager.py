from dataclasses import dataclass
from typing import Optional
import os
import time

import ccxt.async_support as ccxt

from src.executor.feature_state import FeatureState
from src.strategies.taker_trend_strategy import Signal


@dataclass
class AccountSnapshot:
    exchange_id: str
    balance: dict
    positions: list[dict]
    updated_at: int


@dataclass
class ExchangeFill:
    symbol: str
    side: str
    action: str
    price: float
    qty: float
    notional_usd: float
    fee_usd: float
    reason: str
    order_id: Optional[str] = None
    raw_order: Optional[dict] = None


class ExchangeOrderManager:
    """
    ccxt-based exchange gateway.

    This class is intentionally separated from PaperOrderManager:
    paper execution changes only local state, while this class can talk to
    a real exchange account. Keep the real-order switch explicit and boring.
    """

    def __init__(self, logger, exchange_id: str = "binance"):
        self.logger = logger
        self.exchange_id = exchange_id
        self.exchange = None
        self.markets_loaded = False

        # Defaults are deliberately safe:
        # - use_testnet=True tries to route to exchange sandbox/testnet
        # - live_trading_enabled=False means create_order is NOT called
        #
        # To place real orders, both must be changed intentionally:
        # EXECUTION_MODE=live
        # LIVE_TRADING_ENABLED=true
        # ORDER_USE_TESTNET=false
        self.execution_mode = os.getenv("EXECUTION_MODE", "paper").lower()
        self.live_trading_enabled = os.getenv("LIVE_TRADING_ENABLED", "false").lower() == "true"
        self.use_testnet = os.getenv("ORDER_USE_TESTNET", "true").lower() == "true"

        self.account_snapshot = AccountSnapshot()

    async def connect(self):
        if self.exchange is not None:
            return

        if self.exchange_id != "binance":
            raise ValueError(f"unsupported exchange_id for live orders: {self.exchange_id}")

        self.exchange = ccxt.binanceusdm({
            "apiKey": os.getenv("BINANCE_API_KEY"),
            "secret": os.getenv("BINANCE_SECRET"),
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",
                "adjustForTimeDifference": True,
            },
        })

        # Testnet/sandbox is still capable of sending orders to a test account.
        # It is safer than production, but it is not the same as paper trading.
        if self.use_testnet:
            self.exchange.set_sandbox_mode(True)

        await self.exchange.load_markets()
        self.markets_loaded = True

        self.logger.info(
            f"[EXCHANGE_CONNECTED] exchange={self.exchange_id} | "
            f"mode={self.execution_mode} | testnet={self.use_testnet} | "
            f"live_trading_enabled={self.live_trading_enabled}"
        )

    async def close(self):
        if self.exchange is not None:
            await self.exchange.close()
            self.exchange = None
            self.markets_loaded = False

    async def fetch_account_snapshot(self, symbols: list[str]) -> AccountSnapshot:
        await self.connect()

        # fetch_balance gives margin balances and free/used/total funds.
        # For USD-M futures, USDT is usually the key you care about first.
        balance = await self.exchange.fetch_balance()

        # Positions are exchange-specific in shape. Keep the raw dicts now;
        # later you can normalize them into your own PositionState model.
        ccxt_symbols = [self._to_ccxt_future_symbol(symbol) for symbol in symbols]
        positions = await self.exchange.fetch_positions(ccxt_symbols)

        snapshot = AccountSnapshot(
            exchange_id=self.exchange_id,
            balance=balance,
            positions=positions,
            updated_at=int(time.time() * 1000),
        )

        usdt = balance.get("USDT", {})
        self.logger.info(
            f"[ACCOUNT] USDT free={usdt.get('free')} | "
            f"used={usdt.get('used')} | total={usdt.get('total')} | "
            f"positions={len(positions)}"
        )

        return snapshot
    
    async def get_account_snapshot(self):
        return 

    async def execute(self, signal: Signal, state: FeatureState) -> Optional[ExchangeFill]:
        await self.connect()

        if signal.action == "open" and signal.side == "long":
            return await self._open_long(signal, state)

        self.logger.info(f"[EXCHANGE_REJECT][{signal.symbol}] unsupported signal: {signal.side}-{signal.action}")
        return None

    async def _open_long(self, signal: Signal, state: FeatureState) -> Optional[ExchangeFill]:
        symbol = signal.symbol
        snapshot = state.get_snapshot(symbol)
        future_ob = snapshot.get("future_orderbook")
        if not future_ob:
            return None

        ccxt_symbol = self._to_ccxt_future_symbol(symbol)
        price = float(future_ob["ask_price"])
        raw_qty = signal.notional_usd / price
        # 下单数量转换成交易所允许精度的函数
        qty = float(self.exchange.amount_to_precision(ccxt_symbol, raw_qty))

        order_payload = {
            "symbol": ccxt_symbol,
            "type": "market",
            "side": "buy",
            "amount": qty,
            "params": {
                # reduceOnly=False means this order is allowed to open/increase
                # a long position. Never use this path for closing.
                "reduceOnly": False,
            },
        }

        # This is the actual safety gate. If it is false, the code builds and
        # logs the order payload but does not call create_order.
        if not self.live_trading_enabled:
            self.logger.info(f"[DRY_RUN_ORDER] {order_payload}")
            return ExchangeFill(
                symbol=symbol,
                side="long",
                action="open",
                price=price,
                qty=qty,
                notional_usd=qty * price,
                fee_usd=0.0,
                reason="dry-run exchange open long payload generated",
                order_id="DRY_RUN",
                raw_order=order_payload,
            )

        order = await self.exchange.create_order(**order_payload)
        fill_price = self._extract_average_price(order, fallback=price)
        fee_usd = self._extract_fee_usd(order)

        # Only update local state after the exchange accepts the order.
        # Later, this should move to an order/position sync step that confirms
        # filled quantity from the exchange instead of trusting immediate return.
        position = state.get_position(symbol)
        position["side"] = "long"
        position["qty"] = qty
        position["open_fee_usd"] = fee_usd
        position["entry_notional_usd"] = qty * fill_price
        position["entry_price"] = fill_price
        position["entry_time"] = int(time.time() * 1000)
        position["unrealized_pnl"] = 0.0

        return ExchangeFill(
            symbol=symbol,
            side="long",
            action="open",
            price=fill_price,
            qty=qty,
            notional_usd=qty * fill_price,
            fee_usd=fee_usd,
            reason="exchange market open long",
            order_id=str(order.get("id")),
            raw_order=order,
        )

    async def close_long(self, symbol: str, qty: float) -> Optional[dict]:
        await self.connect()

        ccxt_symbol = self._to_ccxt_future_symbol(symbol)
        qty = float(self.exchange.amount_to_precision(ccxt_symbol, qty))

        order_payload = {
            "symbol": ccxt_symbol,
            "type": "market",
            "side": "sell",
            "amount": qty,
            "params": {
                # reduceOnly=True is critical for closing a futures position:
                # it prevents an accidental short if the local position state
                # is wrong or the close quantity is too large.
                "reduceOnly": True,
            },
        }

        if not self.live_trading_enabled:
            self.logger.info(f"[DRY_RUN_CLOSE_ORDER] {order_payload}")
            return order_payload

        return await self.exchange.create_order(**order_payload)

    @staticmethod
    def _to_ccxt_future_symbol(symbol: str) -> str:
        # Your internal symbol is BTC/USDT.
        # Binance USD-M futures in ccxt is usually BTC/USDT:USDT.
        if ":" in symbol:
            return symbol
        base, quote = symbol.split("/")
        return f"{base}/{quote}:{quote}"

    @staticmethod
    def _extract_average_price(order: dict, fallback: float) -> float:
        for key in ("average", "price"):
            value = order.get(key)
            if value:
                return float(value)
        return fallback

    @staticmethod
    def _extract_fee_usd(order: dict) -> float:
        fee = order.get("fee") or {}
        cost = fee.get("cost")
        return float(cost) if cost else 0.0
