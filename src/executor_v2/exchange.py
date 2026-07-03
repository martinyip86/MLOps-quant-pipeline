import ccxt.async_support as ccxt_as
import asyncio
import os
import sys

from logging import Logger
from dotenv import load_dotenv

load_dotenv()

class Exchange:
    def __init__(self,logger:Logger,exchange_id:str="binance"):
        self.logger = logger
        self.exchange_id = exchange_id
        self.exchange = None
        
        self.execution_mode = os.getenv("EXECUTION_MODE")
        self.live_trading_enabled = (int(os.getenv("LIVE_TRADING_ENABLED")) == 1)
        self.order_use_testnet = (int(os.getenv("ORDER_USE_TESTNET")) == 1)
        self.markets_loaded = False

    async def connect(self):
        if self.exchange is not None: return

        if self.exchange_id == "binance":
            self.exchange = ccxt_as.binanceusdm({
                "apiKey":os.getenv("BINANCE_API_KEY"),
                "secret":os.getenv("BINANCE_SECRET"),
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
                self.exchange.set_sandbox_mode(True)

            await self.exchange.load_markets()
            self.markets_loaded = True

            self.logger.info(
                f"[EXCHANGE CONNECTED] exchange={self.exchange_id} | "
                f"mode={self.execution_mode} | testnet={self.order_use_testnet} | "
                f"live_trading_enabled={self.live_trading_enabled}"
            )

    async def get_account_data(self):
        await self.connect()

        