import polars as pl
import asyncio

from logging import Logger
from datetime import datetime,timezone

from src.storage.clickhouse.client import ch_manager

class CkController:
    def __init__(self,logger:Logger):
        self.ch = ch_manager.connect()
        self.logger = logger

    async def insert_executor_orders(self,exchange_id:str,mkt_type:str,order):
        info = order["info"]

        data:dict = {
            "ts":datetime.fromtimestamp(order["timestamp"] / 1000,tz=timezone.utc),
            "order_id":order["id"],
            "client_order_id":order["clientOrderId"],
            "exchange_id":exchange_id,
            "symbol":order["symbol"],
            "mkt_type":mkt_type,
            "side":order["side"],
            "position_side":"long" if order["side"] == "buy" else "short",
            "order_type":order["type"],
            "status":order["status"],
            "price":float(order.get("price") or order.get("average") or 0.0),
            "amount":order["amount"],
            "notional_usd":float(order.get("cost") or 0.0),
            "reduce_only":order["reduceOnly"],
            "signal_id":"",
            "error":"",
        }

        df = pl.DataFrame(data)
        arrow_table = df.to_arrow()
        try:
            await asyncio.to_thread(
                self.ch.insert_arrow,
                table="executor_orders",
                arrow_table=arrow_table
            )
        except Exception as e:
            self.logger.error(f"[INSERT EXECUTOR_ORDERS ERROR] {e}")
        
    async def insert_executor_fill(self,exchange_id:str,mkt_type:str,fill):
        info = fill["info"]

        data:dict = {
            "ts":datetime.fromtimestamp(fill["timestamp"] / 1000,tz=timezone.utc),
            "fill_id":fill["id"],
            "order_id":fill["order"],
            "client_order_id":"",
            "exchange_id":exchange_id,
            "symbol":fill["symbol"],
            "mkt_type":mkt_type,
            "side":fill["side"],
            "price":fill["price"],
            "amount":fill["amount"],
            "cost":fill["cost"],
            "fee_cost":fill["fee"]["cost"],
            "fee_currency":fill["fee"]["currency"],
            "liquidity":fill["takerOrMaker"],
            "trade_id":fill["id"],
        }

        df = pl.DataFrame(data)
        arrow_table = df.to_arrow()
        try:
            await asyncio.to_thread(
                self.ch.insert_arrow,
                table="executor_fill",
                arrow_table=arrow_table
            )
        except Exception as e:
            self.logger.error(f"[INSERT EXECUTOR_FILEE ERROR] {e}")
