from pydantic import Field,BaseModel
from typing import List,Tuple,Optional
from datetime import datetime

class TickData(BaseModel):
    exchange_id:str = Field(...,description="Data source identifier for Smart Order Routing (SOR)")
    symbol:str = Field(...,description="Instrument symbol (e.g., BTC/USDT)")
    mkt_type:str = Field(...,description="Market segment (spot/swap/future)")
    bid_price:float = Field(...,description="Best bid price")
    bid_volume:float = Field(...,description="Best bid quantity")
    ask_price:float = Field(...,description="Best ask price")
    ask_volume:float = Field(...,description="Best ask quantity")
    bid_prices:List[float] = Field(...,description="Array of top 20 bid prices")
    bid_volumes:List[float] = Field(...,description="Array of top 20 bid volumes")
    ask_prices:List[float] = Field(...,description="Array of top 20 ask prices")
    ask_volumes:List[float] = Field(...,description="aArray of top 20 ask volumes")
    nonce:int = Field(...,description="Exchange sequence number/Update ID")
    timestamp:int = Field(...,description="Original exchange matching engine timestamp (ms)")
    local_timestamp:int = Field(default_factory=lambda: int(datetime.now().timestamp() * 1000))

class TradeData(BaseModel):
    exchange_id:str = Field(...,description="Bxchange identifier (e.g., Binance, OKX)")
    symbol:str = Field(...,description="Instrument symbol")
    mkt_type:str = Field(...,description="Market segment (spot/swap/future)")
    trade_id:int = Field(...,description="Unique execution ID from exchange by String Int")
    trade_id_raw:str = Field(...,description="Unique execution ID from exchange by String")
    timestamp:int = Field(...,description="Matching engine execution timestamp (ms)")
    side:str = Field(...,description="Execution direction (buy/sell)")
    price:float = Field(...,description="Execution price")
    amount:float = Field(...,description="Execution quantity")
    is_taker_buyer:bool = Field(...,description="Directional intent: True=Taker Buy (Bullish), False=Taker Sell (Bearish)")
    local_timestamp:int = Field(default_factory=lambda: int(datetime.now().timestamp() * 1000))