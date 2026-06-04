from collections import deque
import json

class FeatureState:
    def __init__(self,symbols:list[str]):
        self.features = {
            symbol:{
                'bid_price_feature':0.0,
                'ask_price_feature':0.0,
                'mid_price_future':0.0,
                'bid_price_spot':0.0,
                'ask_price_spot':0.0,
                'mid_price_spot':0.0,
                'spread_future':0.0,
                'spread_future_bps':0.0,
                'buy_impact_bps_future':0.0,
                'ask_impact_bps_future':0.0,
                'future_book_ofi_1s':0.0,
                'future_book_ofi_2s':0.0,
                'future_trades_flow_1s':0.0,
                'future_trades_flow_2s':0.0,
                'spot_book_ofi_1s':0.0,
                'spot_book_ofi_2s':0.0,
                'spot_trades_flow_1s':0.0,
                'spot_trades_flow_2s':0.0,
                'future_cvd_10s':0.0,
                'future_obi_l1':0.0,
                'future_obi_l3':0.0,
                'future_obi_l5':0.0,
                'spot_obi_l1':0.0,
                'spot_obi_l3':0.0,
                'spot_obi_l5':0.0,
                'future_spot_basis':0.0,
                'future_spot_basis_bps':0.0,
                'premium_discount':0.0,
                'premium_discount_bps':0.0,
                'oi_momentum':0.0,
                'mark_price':0.0
            }
            for symbol in symbols
        }

        self.prev_books = {}
        self.trade_windows = {
            symbol:{
                "future":deque(),
                "spot":deque(),
            }
            for symbol in symbols
        }
        self.oi_windows = {
            symbol:deque()
            for symbol in symbols
        }

    def update_from_redis(self,stream_name:str,content):
        raw = content.get('data')
        if raw is None: return None

        if isinstance(raw,bytes):
            raw = raw.decode()

        data = json.loads(raw)

        symbol = data["symbol"]
        mkt_type = data["mkt_type"]

        if symbol not in self.features:
            return None
        
        if "orderbook" in stream_name:
            self._update_book(symbol,mkt_type,data)

    def _update_book(self,symbol:str,mkt_type:str,data:dict):
        f = self.features[symbol]

        bid_prices = data["bid_prices"]
        bid_volumes = data["bid_volumes"]
        ask_prices = data["ask_prices"]
        ask_volumes = data["ask_volumes"]

        best_bid = bid_prices[0]
        best_ask = ask_prices[0]
        mid = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
        spread_bps = spread / mid * 10000

        prefix = "future" if mkt_type in ["future", "swap"] else "spot"

        if prefix == "future":
            f["bid_price_feature"] = best_bid
            f["ask_price_feature"] = best_ask
            f["mid_price_feature"] = mid
            f["spread_future"] = spread
            f["spread_future_bps"] = spread_bps

            f["future_obi_l1"] = self._obi(bid_volumes,ask_volumes,1)
            f["future_obi_l3"] = self._obi(bid_volumes,ask_volumes,3)
            f["future_obi_l5"] = self._obi(bid_volumes,ask_volumes,5)

            f["buy_impact_bps_future"] = self._impact_bps(ask_prices,ask_volumes,mid,target_qty=1.0)
            f["ask_impact_bps_future"] = self._impact_bps(bid_prices,bid_volumes,mid,target_qty=1.0)

        else:
            f["bid_price_spot"] = best_bid
            f["ask_price_spot"] = best_ask
            f["mid_price_spot"] = mid

            f["spot_obi_l1"] = self._obi(bid_volumes,ask_volumes,1)
            f["spot_obi_l3"] = self._obi(bid_volumes,ask_volumes,3)
            f["spot_obi_l5"] = self._obi(bid_volumes,ask_volumes,5)

        prev_key = (symbol,prefix)
        prev = self.prev_books.get(prev_key)

        if prev is not None:
            ofi = self._book_ofi(
                prev_bid=prev["best_bid"],
                prev_bid_size=prev["bid_size"],
                prev_ask=prev["best_ask"],
                prev_ask_size=prev["ask_size"],
                bid=best_bid,
                bid_size=bid_volumes[0],
                ask=best_ask,
                ask_size=ask_volumes[0],
            )

    @staticmethod
    def _obi(bid_volumes:list,ask_volumes:list,level:int):
        bid_sum = sum(bid_volumes[:level])
        ask_sum = sum(ask_volumes[:level])

        return (bid_sum - ask_sum) / (bid_sum + ask_sum)
    
    @staticmethod
    def _impact_bps(prices:list,volumes:list,mid_price:float,target_qty:float):
        remaining = target_qty
        notional = 0.0
        filled = 0.0

        for price,volume in zip(prices,volumes):
            take_qty = min(remaining,volume)
            notional += price * take_qty
            filled += take_qty
            remaining -= take_qty

            if remaining <= 0: break

        if filled <= 0.0 or mid_price <= 0.0: return 0.0

        avg_price = notional / filled
        return abs(avg_price - mid_price) / mid_price * 10000
    
    @staticmethod
    def _book_ofi(prev_bid,prev_bid_size,prev_ask,prev_ask_size,bid,bid_size,ask,ask_size):
        bid_ofi = 0.0
        ask_ofi = 0.0

        if bid > prev_bid:
            bid_ofi = bid_size
        elif bid == prev_bid:
            bid_ofi = bid_size - prev_bid_size
        else:
            bid_ofi = -prev_bid_size

        if ask < prev_ask:
            ask_ofi = ask_size
        elif ask == prev_ask:
            ask_ofi = ask_size - prev_ask_size
        else:
            ask_ofi = -prev_ask_size

        return bid_ofi + ask_ofi
