from src.collectors.binance.spot import BinanceSpotWsManager
from src.collectors.binance.future import BinanceFutureManager
from src.collectors.okx.spot import OkxSpotWsManager
from src.collectors.okx.future import OkxFutureManager
from src.monitoring.pusher import start_metrics_pusher
import os
import asyncio
import argparse

class Manager:
    def __init__(self,exchange_id:str):
        self.exchange_id:str = exchange_id
        self.mkt_types = ['spot','future']
        self.symbols = ['BTC/USDT','ETH/USDT','SOL/USDT','XRP/USDT']
        self._collector_map = {
            ('binance','spot'):BinanceSpotWsManager,
            ('okx','spot'):OkxSpotWsManager,
            ('binance','future'):BinanceFutureManager,
            ('okx','future'):OkxFutureManager,
        }

    async def main(self):
        tasks = []
        for mkt_type in self.mkt_types:
            collector_class = self._collector_map.get((self.exchange_id,mkt_type))
            if not collector_class:
                print(f"Error: {self.exchange_id} {mkt_type} 不在支持列表中")
                return
            controller = collector_class(self.exchange_id,mkt_type)
            await controller.connect()
                    
            for symbol in self.symbols:
                if mkt_type == 'spot':
                    tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_order_book','orderbook')))
                    tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_trades','trades')))
                if mkt_type == 'future':
                    tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_order_book','orderbook')))
                    tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_trades','trades')))
                    tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_mark_price','mark_price')))
                    tasks.append(asyncio.create_task(controller.fetch_open_interest(symbol,30)))
                    tasks.append(asyncio.create_task(controller.watch_liquidations(symbol)))
                    if self.exchange_id == 'okx':
                        tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_funding_rate','funding_rate')))

            tasks.append(asyncio.create_task(controller.route()))
            # tasks.append(asyncio.create_task(controller.start_health_check()))
                   
        tasks.append(asyncio.create_task(start_metrics_pusher(job_name=f"market_collector_{self.exchange_id}")))
        
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--exchange',type=str,default=os.getenv('EXCHANGE', 'binance'))
    # parser.add_argument('--type',type=str,default=os.getenv('TYPE', 'spot'))
    args = parser.parse_args()
    manager = Manager(exchange_id=args.exchange)
    
    try:
        asyncio.run(manager.main())
    except KeyboardInterrupt:
        print("停止采集...")