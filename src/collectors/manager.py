from src.collectors.binace.spot import BinanceSpotWsManager
from src.collectors.okx.spot import OkxSpotWsManager
from src.monitoring.pusher import start_metrics_pusher
import os
import asyncio
import argparse

class Manager:
    def __init__(self,exchange_id:str,mkt_type:str):
        self.exchange_id:str = exchange_id
        self.mkt_type:str = mkt_type
        self.symbols = ['BTC/USDT','ETH/USDT']
        self._collector_map = {
            ('binance','spot'):BinanceSpotWsManager,
            ('okx','spot'):OkxSpotWsManager,
        }

    async def main(self):
        tasks = []
        collector_class = self._collector_map.get((self.exchange_id,self.mkt_type))
        if not collector_class:
            print(f"Error: {self.exchange_id} {self.mkt_type} 不在支持列表中")
            return
        controller = collector_class(self.exchange_id,self.mkt_type)
        await controller.connect()
                
        for symbol in self.symbols:
            tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_order_book')))
            tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_trades')))

        tasks.append(asyncio.create_task(controller.route()))
        tasks.append(asyncio.create_task(start_metrics_pusher(job_name=f"market_collector_{self.exchange_id}_{self.mkt_type}")))
        tasks.append(asyncio.create_task(controller.start_health_check()))
                   
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--exchange',type=str,default=os.getenv('EXCHANGE', 'binance'))
    parser.add_argument('--type',type=str,default=os.getenv('TYPE', 'spot'))
    args = parser.parse_args()
    manager = Manager(exchange_id=args.exchange,mkt_type=args.type)
    
    try:
        asyncio.run(manager.main())
    except KeyboardInterrupt:
        print("停止采集...")