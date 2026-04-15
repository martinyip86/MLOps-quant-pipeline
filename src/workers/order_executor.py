from src.utils.logger import setup_logger
from src.storage.redis.client import redis_manager
import ccxt.async_support as ccxt
from dotenv import load_dotenv
import os
import orjson
import asyncio

load_dotenv()

class OrderExecutor:
    def __init__(self,exchange_id:str='binance',mkt_type:str='spot',symbol:str='BTC/USDT'):
        self.exchange_id = exchange_id
        self.mkt_type = mkt_type
        self.symbol = symbol
        self.logger = setup_logger("executor",f"logs/executor/executor_{exchange_id}.log")
        self.exchange = getattr(ccxt,exchange_id)({
            'apiKey': os.getenv('BINANCE_API_KEY'),
            'secret': os.getenv('BINANCE_SECRET'),
            'enableRateLimit': True,
            'options': {'defaultType': mkt_type}
        })

        self.last_market_price = 0.0
        self.position = 0.0
        self.max_pos_usd = 1000.0
        self.min_confidence = 1.5
        self.daily_pnl = 0.0
        self.max_daily_loss = -500.0
        self.is_melted = False
        self.latency_threshold = 1.0
        self.entry_price = 0.0    # 持仓入场均价
        self.position_amount = 0.0 # 持仓数量
        self.realized_pnl = 0.0    # 已实现盈亏 (USDT)
        self.fee_rate = 0.0002     # 假设手续费 2bps

    def current_pnl(self):
        """
        动态属性：计算当前总盈亏 (实现 + 未实现)
        """
        # 这里需要最新的市价。在生产中，我们会从 self.last_tick 获取
        # 简单演示：假设我们有一个 self.last_market_price
        unrealized_pnl = 0.0
        if self.position_amount != 0:
            # 浮动盈亏 = (现价 - 入场价) * 数量 (做多情况)
            # 如果是做空，则是 (入场价 - 现价) * 数量
            unrealized_pnl = (self.last_market_price - self.entry_price) * self.position_amount

        return self.realized_pnl + unrealized_pnl
    
    def update_position(self,deal_price,amount,side):
        """
        每当交易成交时，更新持仓状态
        side: 'BUY' 或 'SELL'
        """
        trade_cost = deal_price * amount * self.fee_rate # 估算手续费

        if side == "BUY":
            # 更新持仓均价 (简化版)
            new_total_qty = self.position_amount + amount
            self.entry_price = ((self.entry_price * self.position_amount) + (deal_price * amount)) / new_total_qty
            self.position_amount = new_total_qty
            self.realized_pnl -= trade_cost # 扣除买入手续费
        else:
            # 卖出时计算实现盈亏
            profit = (deal_price - self.entry_price) * amount
            self.realized_pnl += (profit - trade_cost)
            self.position_amount -= amount
            if self.position_amount == 0:
                self.entry_price = 0.0

    async def check_kill_switch(self,current_pnl,latency):
        if current_pnl < self.max_daily_loss:
            self.is_melted = True
            return True,"Daily PnL Threshold Reached"
        
        if latency > self.latency_threshold:
            return True,f"High Latency: {latency}s"
        
        return False,""

    async def get_balance(self):
        balance = await self.exchange.fetch_balance()
        return balance['free']['USDT']
    
    async def execute_logic(self,signal:dict):
        print(signal)
        best_bid = signal['metadata'].get('best_bid',0)
        best_ask = signal['metadata'].get('best_ask',0)
        self.last_market_price = (best_bid + best_ask) / 2 if (best_bid + best_ask) > 0 else 0

        latency = (asyncio.get_event_loop().time() - signal['timestamp'] / 1000)
        melted,reason = await self.check_kill_switch(self.current_pnl(),latency)

        if melted:
            self.logger.critical(f"🚨 [CIRCUIT BREAKER] {reason}! Emergency Stop.")
            # 如果有持仓，立刻以市价平仓撤离
            # if self.position != 0:
            #     await self.emergency_exit()
            return
        
        available_usdt = await self.get_balance()
        # if available_usdt < 10:
        #     self.logger.warning("操作停止：USDT 余额不足")
        #     return

        recommendation = signal['recommendation']
        confidence = signal['confidence']
        metadata = signal.get('metadata',{})

        print(recommendation)
        print(confidence)
        # 1. 基础过滤
        if recommendation == 'NEUTRAL' or confidence < self.min_confidence:
            return
        
        # 2. 获取实时价格
        ticker = await self.exchange.fetch_ticker(self.symbol)
        last_price = ticker['last']

        # 3. 风险管理 (Position Sizing)
        # 根据信心度动态计算下单大小 (Kelly Criterion 简化版)
        # 信心越高，下单越重，但不超过最大持仓
        order_size_usd = min(self.max_pos_usd * (confidence / 5.0),self.max_pos_usd)
        amount = order_size_usd / last_price

        # 4. 执行保护 (Slippage Calibration)
        # 如果实时 Spread 大于我们在 Consolidator 里设定的阈值，放弃执行
        
        if metadata.get('spread_bps',0) > 10.0:
            self.logger.warning(f"⚠️ 滑点过高({metadata['spread_bps']}), 放弃下单")
            return
        
        try:
            if recommendation == 'LONG' and self.position_amount <= 0:
                self.logger.info(f"🚀 [BUY] {self.symbol} | Amount: {amount:.4f} | Conf: {confidence:.2f}")
                # order = await self.exchange.create_market_buy_order(self.symbol, amount)
                deal_price = last_price
                self.update_position(deal_price, amount, "BUY") # <--- 这里调用了！

            elif recommendation == 'SHORT' and self.position_amount >= 0:
                self.logger.info(f"📉 [SELL] {self.symbol} | Amount: {amount:.4f} | Conf: {confidence:.2f}")
                # order = await self.exchange.create_market_sell_order(self.symbol, amount)
                deal_price = last_price # 模拟成交价
                self.update_position(deal_price, amount, "SELL") # <--- 这里调用了！

            self.logger.info(
                f"📊 [战报] 持仓: {self.position_amount:.4f} | "
                f"入场均价: {self.entry_price:.2f} | "
                f"现价: {self.last_market_price:.2f} | "
                f"今日总盈亏: {self.current_pnl():.4f} USDT"
            )

        except Exception as e:
            self.logger.error(f"❌ 下单失败: {e}")

    async def emergency_exit(self):
        """
        紧急清仓：不计价格，立刻把手里持有的币换成钱
        """
        self.logger.critical("🚨 执行紧急清仓程序...")
        
        if abs(self.position_amount) < 0.0001: # 几乎没持仓
            return

        try:
            if self.position_amount > 0:
                # 持有多头，直接卖出
                # 这里的数量必须是绝对值，且满足交易所最小精度
                await self.exchange.create_market_sell_order(self.symbol, self.position_amount)
                self.logger.info(f"⚡ 紧急平多成功，数量: {self.position_amount}")
            elif self.position_amount < 0:
                # 持有空头，直接买回
                await self.exchange.create_market_buy_order(self.symbol, abs(self.position_amount))
                self.logger.info(f"⚡ 紧急平空成功，数量: {abs(self.position_amount)}")
            
            # 清空本地状态
            self.position_amount = 0.0
            self.entry_price = 0.0
            
        except Exception as e:
            self.logger.error(f"❌ 紧急清仓失败! 必须手动干预: {e}")

    async def main(self):
        redis = redis_manager.connect
        stream_key = f"alpha:signals:{self.exchange_id}:spot:{self.symbol.replace('/','-')}"
        group_name = "executor_group"
        consumer_name = "executor_node_1"

        try:
            await redis.xgroup_create(stream_key,group_name,id='0',mkstream=True)
        except: pass

        self.logger.info(f"🎧 执行器就绪，开始监听信号: {stream_key}")

        while True:
            # try:
                response = await redis.xreadgroup(
                    groupname=group_name,
                    consumername=consumer_name,
                    streams={stream_key:">"},
                    count=1,
                    block=2000
                )

                if not response:
                    # 即使没信号，也要定期打印当前持仓的浮动盈亏
                    if self.position_amount != 0:
                        self.logger.info(f"⏳ 监控中... 当前持仓浮盈: {self.current_pnl():.4f} USDT")

                if response:
                    for _,messages in response:
                        for msg_id,content in messages:
                            signal = orjson.loads(content['data'])
                            await self.execute_logic(signal)
                            await redis.xack(stream_key, group_name, msg_id)
            # except Exception as e:
            #     self.logger.error(f"Loop Error: {e}")
            #     await asyncio.sleep(1)
            # finally:
            #     # 楚格准则：始终记得关闭异步连接
            #     await self.exchange.close()
            #     pass

if __name__=='__main__':
    order_executor =OrderExecutor()
    asyncio.run(order_executor.main())