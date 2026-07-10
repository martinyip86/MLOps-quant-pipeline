import ccxt.async_support as ccxt_as
import asyncio
import os

from src.storage.redis.client import redis_manager
from src.executor_v2.ck_controller import CkController
from logging import Logger
from dotenv import load_dotenv

load_dotenv()

class Exchange:
    def __init__(self,logger:Logger,symbols:list[str],exchange_id:str="binance"):
        self.logger = logger
        self.redis = redis_manager.connect
        self.exchange_id = exchange_id
        self.exchange = None
        self.symbols = symbols
        
        self.execution_mode = os.getenv("EXECUTION_MODE")
        self.live_trading_enabled = (int(os.getenv("LIVE_TRADING_ENABLED")) == 1)
        self.order_use_testnet = (int(os.getenv("ORDER_USE_TESTNET")) == 1)
        self.markets_loaded = False

        self.ck_controller = CkController(self.logger)

    async def connect(self):
        if self.exchange is not None: return

        if self.exchange_id == "binance":
            self.exchange = ccxt_as.binanceusdm({
                "apiKey":os.getenv("BINANCE_API_KEY" if not self.order_use_testnet else "BINANCE_DEMO_API_KEY"),
                "secret":os.getenv("BINANCE_SECRET" if not self.order_use_testnet else "BINANCE_DEMO_SECRET"),
                "enableRateLimit":True,
                "options":{
                    "defaultType":"swap",
                    "adjustForTimeDifference":True,
                    "ws":{
                        "heartbeat":20000,
                    }
                }
            })
            
            if self.order_use_testnet:
                self.exchange.enable_demo_trading(True)

            await self.exchange.load_markets()
            self.markets_loaded = True

            self.logger.info(
                f"[EXCHANGE CONNECTED] exchange={self.exchange_id} | "
                f"mode={self.execution_mode} | testnet={self.order_use_testnet} | "
                f"live_trading_enabled={self.live_trading_enabled}"
            )

    async def close(self):
        if self.exchange is not None:
            self.exchange.close()

    async def get_account_data(self) -> dict[str,dict]:
        await self.connect()

        balance = await self.get_balance()
        position = await self.get_position()
        
        return {
            "balance":balance,
            "position":position,
        }
        
    async def get_balance(self) -> dict:
        await self.connect()
        balance = await self.exchange.fetch_balance()
        info = balance["info"]

        return {
            "asset":"USDT",
            "free_usdt":float(balance["USDT"]["free"]),                 # 可使用金额
            "used_usdt":float(balance["USDT"]["used"]),                 # 已占用金额
            "total_usdt":float(balance["USDT"]["total"]),               # 总金额
            "wallet_balance":float(info["totalWalletBalance"]),         # 钱包余额
            "available_balance":float(info["availableBalance"]),        # 可用余额
            "unrealized_pnl":float(info["totalUnrealizedProfit"]),      # 当前浮动盈亏
            "margin_balance":float(info["totalMarginBalance"]),         # 保证金余额 一般约等于 wallet + unrealized pnl          
        }
    
    async def get_order(self,symbol:str,order_id:str) -> dict:
        await self.connect()
        orders = await self.exchange.fetch_my_trades(
            symbol=symbol,
            since=None,
            limit=50
        )
        return [order for order in orders if order["order"] == order_id]
    
    async def get_open_order(self,symbol:str):
        orders = await self.exchange.fetch_open_orders(symbol)
        print(orders)
    
    async def get_position(self) -> dict:
        await self.connect()
        swap_symbols = [f"{symbol}:{symbol.split('/')[-1]}" for symbol in self.symbols]
        try:
            data:dict = {}
            positions = await self.exchange.fetch_positions(swap_symbols)
            if positions:
                for position in positions:
                    info = position["info"]
                    if position["symbol"] not in data:
                        data[position["symbol"]] = {
                            "symbol":position["symbol"],
                            "side":position["side"],
                            "contracts":float(position["contracts"]),
                            "contract_size":float(position["contractSize"]),
                            "entry_price":float(position["entryPrice"]),
                            "mark_price":float(position["markPrice"]),
                            "break_even_price":float(info["breakEvenPrice"]),
                            "notional":float(position["notional"]),
                            "unrealized_pnl":float(position["unrealizedPnl"]),
                            "initial_margin":float(position["initialMargin"]),
                            "maintenance_margin":float(position["maintenanceMargin"]),
                            "liquidation_price":float(position["liquidationPrice"]),
                            "margin_mode":position["marginMode"],
                            "timestamp":position["timestamp"]
                        }

            return data
                    
        except Exception as e:
            print(f"Error: {e}")
        
    async def open_position(self,symbol:str,side:str,amount:float) -> dict:
        """
        市价开仓
        side:
            long  -> buy
            short -> sell
        """
        await self.connect()

        if side == "long":
            order_side = "buy"
        elif side == "short":
            order_side = "sell"
        else:
            raise ValueError(f"invalid side: {side}")
        
        try:
            order = await self.exchange.create_order(
                symbol=symbol,
                type="market",
                side=order_side,
                amount=amount,
                params={
                    "reduceOnly":False
                }
            )
            await self.ck_controller.insert_executor_orders(
                exchange_id=self.exchange_id,
                mkt_type="swap",
                order=order
            )

            self.logger.info(f"[OPEN ORDER] {order}")
            return order
        except Exception as e:
            self.logger.exception(f"[OPEN ORDER ERROR] {e}")
            raise

    async def close_position(self,symbol:str,side:str,amount:float) -> dict:
        """
        市价平仓
        side:
            long  -> 平多，用 sell
            short -> 平空，用 buy
        """
        await self.connect()

        if side == "long":
            order_side = "sell"
        elif side == "short":
            order_side = "buy"
        else:
            raise ValueError(f"invalid order {side}")
        
        try:
            order = await self.exchange.create_order(
                symbol=symbol,
                type="market",
                side=order_side,
                amount=amount,
                params={
                    "reduceOnly": True
                }
            )

            await self.ck_controller.insert_executor_orders(
                exchange_id=self.exchange_id,
                mkt_type="swap",
                order=order
            )

            self.logger.info(f"[CLOSE ORDER] {order}")
            return order
        except Exception as e:
            self.logger.exception(f"[OPEN ORDER ERROR] {e}")
            raise