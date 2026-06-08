from collections import deque
import time

class DataManager:
    def __init__(self,symbols:list):
        self.symbols = symbols

        self.data_bucket = {
            symbol:{
                'orderbook':{
                    'spot':deque(),
                    'future':deque()
                },
                'trades':{
                    'spot':deque(),
                    'future':deque()
                },
                'market_price':{
                    'future':deque()
                },
                'open_interest':{
                    'future':deque()
                }
            }
            for symbol in symbols
        }

    def _update_orderbook(self,symbol:str,mkt_type:str,data_type:str,data:dict):
        data_bucket = self.data_bucket[symbol][data_type][mkt_type]
        data_bucket.append({
            'timestamp':data['timestamp'],
            'bid_price':data['bid_prices'][0],
            'bid_volume':data['bid_volumes'][0],
            'ask_price':data['ask_prices'][0],
            'ask_volume':data['ask_volumes'][0],
            'bid_prices':data['bid_prices'],
            'bid_volumes':data['bid_volumes'],
            'ask_prices':data['ask_prices'],
            'ask_volumes':data['ask_volumes'],
            'micro_price':self._micro_price(data['bid_prices'][0],data['bid_volumes'][0],data['ask_prices'][0],data['ask_volumes'][0]),
            'mid_price':self._mid_price(data['bid_prices'][0],data['ask_prices'][0]),
            'spread':data['ask_prices'][0] - data['bid_prices'][0],
            'obi_l1':self._obi(data['bid_volumes'],data['ask_volumes'],level=1),
            'obi_l5':self._obi(data['bid_volumes'],data['ask_volumes'],level=5),
        })
        now_ms = data['timestamp']
        while data_bucket and data_bucket[0]["timestamp"] < (now_ms - 3000):
            data_bucket.popleft()

    @staticmethod
    def _micro_price(bid_price,bid_volume,ask_price,ask_volume) -> float:
        return (bid_price * ask_volume + ask_price * bid_volume) / (bid_volume + ask_volume)
    
    @staticmethod
    def _mid_price(bid_price,ask_price) -> float:
        return (bid_price + ask_price) / 2
    
    @staticmethod
    def _obi(bid_volumes:list,ask_volumes:list,level:int) -> float:
        bid_sum = sum(bid_volumes[:level])
        ask_sum = sum(ask_volumes[:level])

        return (bid_sum - ask_sum) / (bid_sum + ask_sum)
    
    def _update_trades(self,symbol:str,mkt_type:str,data_type:str,data:dict):
        data_bucket = self.data_bucket[symbol][data_type][mkt_type]

        side = data['side']
        turnover = data['price'] * data['amount']

        signed_turnover = turnover if side == 'buy' else -turnover
        signed_amount = data['amount'] if side == 'buy' else -data['amount']

        data_bucket.append({
            'timestamp':data['timestamp'],
            'price':data['price'],
            'amount':data['amount'],
            'side':side,
            'turnover':turnover,
            'signed_turnover':signed_turnover,
            'signed_amount':signed_amount,
        })
        now_ms = data['timestamp']
        while data_bucket and data_bucket[0]["timestamp"] < (now_ms - 3000):
            data_bucket.popleft()

    def _update_market_price(self,symbol:str,mkt_type:str,data_type:str,data:dict):
        data_bucket = self.data_bucket[symbol][data_type][mkt_type]
        data_bucket.append({
            'timestamp':data['timestamp'],
            'mark_price':data['mark_price'],
        })
        now_ms = data['timestamp']
        while data_bucket and data_bucket[0]["timestamp"] < (now_ms - 3000):
            data_bucket.popleft()

    def _update_open_interest(self,symbol:str,mkt_type:str,data_type:str,data:dict):
        data_bucket = self.data_bucket[symbol][data_type][mkt_type]
        data_bucket.append({
            'timestamp':data['timestamp'],
            'open_interest_amount':data['open_interest_amount'],
        })
        now_ms = data['timestamp']
        while data_bucket and data_bucket[0]["timestamp"] < (now_ms - 3000):
            data_bucket.popleft()

    def generate_features(self,symbol:str):
        data_bucket = self.data_bucket[symbol]
        orderbook_spot = data_bucket['orderbook']['spot']
        orderbook_future = data_bucket['orderbook']['future']
        trades_spot = data_bucket['trades']['spot']
        trades_future = data_bucket['trades']['future']
        mark_price = data_bucket['market_price']['future']
        open_interest = data_bucket['open_interest']['future']

        for dt in [orderbook_spot,orderbook_future,trades_spot,trades_future]:
            if not self._is_window_ready(dt,window_ms=2000): return None

        features = {
            'best_bid':orderbook_future[-1]['bid_price'],
            'best_ask':orderbook_future[-1]['ask_price'],
            'micro_price_future':orderbook_future[-1]['micro_price'],
            'mid_price_future':orderbook_future[-1]['mid_price'],
            'mid_price_spot':orderbook_spot[-1]['mid_price'],
            'spread_future':orderbook_future[-1]['spread'],
            'spot_obi_l1':orderbook_spot[-1]['obi_l1'],
            'spot_obi_l5':orderbook_spot[-1]['obi_l5'],
            'future_obi_l1':orderbook_future[-1]['obi_l1'],
            'future_obi_l5':orderbook_future[-1]['obi_l5'],
            'mark_price':mark_price[-1]['mark_price'] if mark_price else None,
            'open_interest':open_interest[-1]['open_interest_amount'] if open_interest else None,
            'spot_ob_ofi_1s':self._ofi_window(orderbook_spot,window_ms=1000),
            'spot_ob_ofi_2s':self._ofi_window(orderbook_spot,window_ms=2000),
            'future_ob_ofi_1s':self._ofi_window(orderbook_future,window_ms=1000),
            'future_ob_ofi_2s':self._ofi_window(orderbook_future,window_ms=2000),
            'future_trade_flow_1s':self._trade_flow_window(trades_future,window_ms=1000),
            'future_trade_flow_2s':self._trade_flow_window(trades_future,window_ms=2000),
            'spot_trade_flow_1s':self._trade_flow_window(trades_spot,window_ms=1000),
            'spot_trade_flow_2s':self._trade_flow_window(trades_spot,window_ms=2000)
        }

        snapshot = {
            "spot_orderbook":orderbook_spot[-1],
            "future_orderbook":orderbook_future[-1]
        }

        return symbol,features,snapshot

    @staticmethod
    def _is_window_ready(bucket:deque,window_ms:int) -> bool:
        if not bucket: return False

        return (bucket[-1]['timestamp'] - bucket[0]['timestamp']) >= window_ms
    
    def _ofi_window(self,orderbook:deque,window_ms:int) -> float:
        if not orderbook: return 0.0

        now_ms = orderbook[-1]['timestamp']
        start_ms = now_ms - window_ms

        rows = [
            row for row in orderbook
            if row['timestamp'] >= start_ms
        ]

        return self._ofi(rows)
    
    @staticmethod
    def _ofi(orderbook:deque) -> float:
        total = 0.0

        rows = list(orderbook)

        for prev,cur in zip(rows,rows[1:]):
            bid_ofi = 0.0
            ask_ofi = 0.0

            if cur['bid_price'] > prev['bid_price']:
                bid_ofi = cur['bid_volume']
            elif cur['bid_price'] == prev['bid_price']:
                bid_ofi = cur['bid_volume'] - prev['bid_volume']
            else:
                bid_ofi = -prev['bid_volume']

            if cur['ask_price'] < prev['ask_price']:
                ask_ofi = -cur['ask_volume']
            elif cur['ask_price'] == prev['ask_price']:
                ask_ofi = -(cur['ask_volume'] - prev['ask_volume'])
            else:
                ask_ofi = prev['ask_volume']

            total += bid_ofi + ask_ofi

        return total
    
    def _trade_flow_window(self,trades:deque,window_ms:int) -> float:
        if not trades: return 0.0

        now_ms = trades[-1]['timestamp']
        start_ms = now_ms - window_ms

        total = 0.0

        for row in trades:
            if row['timestamp'] >= start_ms:
                total += row['signed_turnover']

        return total

    def main(self,stream_key:str,data):
        part = stream_key.split(":")
        data_type = part[-1]
        symbol = part[-2].replace('-','/')
        mkt_type = part[-3]
        if symbol in self.symbols:
            if data_type == 'orderbook':
                self._update_orderbook(symbol,mkt_type,data_type,data)
            
            elif data_type == 'trades':
                self._update_trades(symbol,mkt_type,data_type,data)

            elif data_type == 'market_price':
                self._update_market_price(symbol,mkt_type,data_type,data)

            elif data_type == 'open_interest':
                self._update_open_interest(symbol,mkt_type,data_type,data)

            return self.generate_features(symbol)
        
        return None