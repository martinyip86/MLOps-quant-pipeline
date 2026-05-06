import ccxt.pro as ccxt_pro
import asyncio
import polars as pl

async def main():
    symbol = 'BTC/USDT:USDT'
    exchange = ccxt_pro.binanceusdm({
        'enableRateLimit':True,
        'options':{
            'defaultType':'future'
        }
    })
    ticker = await exchange.watch_ticker(symbol)
    print("tick:")
    print(ticker)

    mark_price = await exchange.watch_mark_price(symbol)
    print("mark_price")
    print(mark_price)

    oi = await exchange.fetch_open_interest(symbol)
    print("oi")
    print(oi)

    trades = await exchange.watch_trades(symbol)
    print("trades")
    print(trades)

    await exchange.close()
    


if __name__ == '__main__':
    asyncio.run(main())