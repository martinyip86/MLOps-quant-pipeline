from src.workers.trades_spot_patcher import TradesSpotPatcher
from src.workers.trades_future_patcher import TradesFuturetPatcher
from src.workers.mark_price_future_patcher import MarkPriceFuturetPatcher
from src.utils.logger import setup_logger
import time

class DailyPatcher:
    def __init__(self,target_date:str=None):
        self.target_date = target_date
        self.exchange_ids = ['binance']
        self.symbols = ['BTC/USDT','ETH/USDT','SOL/USDT','XRP/USDT']
        self.logger = setup_logger(
            name='daily.patcher',
            log_file='logs/workers/daily_patcher.log'
        )

    def main(self):
        for exchange_id in self.exchange_ids:
            for symbol in self.symbols:
                TradesSpotPatcher(exchange_id,symbol,self.target_date,self.logger).main()
                time.sleep(5)
                TradesFuturetPatcher(exchange_id,symbol,self.target_date,self.logger).main()
                time.sleep(5)
                MarkPriceFuturetPatcher(exchange_id,symbol,self.target_date,self.logger).main()
                time.sleep(5)

if __name__ == '__main__':
    obj = DailyPatcher()
    obj.main()