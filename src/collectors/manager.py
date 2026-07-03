from src.collectors.binance.spot import BinanceSpotWsManager
from src.collectors.binance.swap import BinanceSwapManager
from src.collectors.okx.spot import OkxSpotWsManager
from src.collectors.okx.swap import OkxSwapManager
from src.monitoring.pusher import start_metrics_pusher
from aiohttp import web
import os
import asyncio
import argparse
import time

class Manager:
    def __init__(self,exchange_id:str):
        self.exchange_id:str = exchange_id
        self.mkt_types = ['spot','swap']
        self.symbols = ['BTC/USDT','ETH/USDT','SOL/USDT','XRP/USDT']
        self._collector_map = {
            ('binance','spot'):BinanceSpotWsManager,
            ('okx','spot'):OkxSpotWsManager,
            ('binance','swap'):BinanceSwapManager,
            ('okx','swap'):OkxSwapManager,
        }

    async def main(self):
        tasks = []
        controllers = []
        for mkt_type in self.mkt_types:
            collector_class = self._collector_map.get((self.exchange_id,mkt_type))
            if not collector_class:
                print(f"Error: {self.exchange_id} {mkt_type} 不在支持列表中")
                return
            controller = collector_class(self.exchange_id,mkt_type)
            controllers.append(controller)
            try:
                await controller.connect()
            except Exception as e:
                # Do not abort the whole process on startup connection failure.
                # watch_loop and the periodic swap tasks will keep retrying.
                controller.logger.error(f"initial connect failed, background tasks will retry: {e}")
                    
            for symbol in self.symbols:
                if mkt_type == 'spot':
                    tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_order_book','orderbook')))
                    tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_trades','trades')))
                if mkt_type == 'swap':
                    tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_order_book','orderbook')))
                    tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_trades','trades')))
                    tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_mark_price','mark_price')))
                    tasks.append(asyncio.create_task(controller.fetch_open_interest(symbol,30)))
                    tasks.append(asyncio.create_task(controller.watch_liquidations(symbol)))
                    if self.exchange_id == 'okx':
                        tasks.append(asyncio.create_task(controller.watch_loop(symbol, 'watch_funding_rate','funding_rate')))

            tasks.append(asyncio.create_task(controller.route()))
                   
        tasks.append(asyncio.create_task(start_metrics_pusher(job_name=f"market_collector_{self.exchange_id}")))
        tasks.append(asyncio.create_task(self.start_health_check(controllers)))
        
        await asyncio.gather(*tasks)

    async def start_health_check(self,controllers:list,port:int=8080):
        async def handle(_request):
            now = time.time()
            stale = [
                f"{controller.exchange_id}:{controller.mkt_type}:{now - controller.last_time:.1f}s"
                for controller in controllers
                if now - controller.last_time > 120
            ]

            if stale:
                return web.Response(status=500,text=f"Data Silence: {', '.join(stale)}")

            return web.Response(status=200,text="OK")
        
        app = web.Application()
        app.router.add_get('/health',handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner,'0.0.0.0',port)
        print(f"✅ Manager health check server started at : {port}/health")
        await site.start()

        # Keep the health server task alive for the lifetime of the collector.
        while True:
            await asyncio.sleep(3600)

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
