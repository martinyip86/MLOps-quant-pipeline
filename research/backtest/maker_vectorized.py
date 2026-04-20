import polars as pl
import numpy as np

class MakerVectorized:
    def __init__(self):
        self.maker_fee = 0.0
        self.slippage = 0.0

    def backtest(self,df:pl.DataFrame,weights:dict,threshold:float=2.0,latency_ticks: int = 3):
        coef = weights['coef']
        intercept = weights['intercept']
        scale = weights['signal_scale']
        lag = weights['best_lag']

        # 1. 计算信号 (与 Taker 一致)
        df_bt = df.with_columns([
            ((pl.col('vamp_bias') * coef[0] + pl.col('ofi') * coef[1] + pl.col('imbalance') * coef[2] + intercept) / scale).alias('z_score')
        ])
        # 2. Maker 挂单决策
        # 当信号 > threshold 时，我们在买一 (bid_prices[0]) 挂单买入
        # 我们需要判断：在未来一段时间内，价格是否“跌”到了我们的挂单价
        df_bt = df_bt.with_columns([
            pl.when(pl.col('z_score') > threshold).then(1).when(pl.col('z_score') < -threshold).then(-1).otherwise(0).shift(latency_ticks).alias('side')
        ])
        # 3. 成交判定 (Maker 的灵魂)
        # 简化模型：如果未来 1 个 tick 的价格触碰了我们的挂单位置，则认为成交
        # 买单成交条件：未来的卖一价 (ask_price) <= 我们的挂买价 (bid_price)
        # 实际上，通常简化为：如果下一时刻价格有向下波动，则买单成交
        df_bt = df_bt.with_columns([
            pl.col('mid_price').diff().alias('price_change')
        ]).with_columns([
            (
                pl.when((pl.col('side') == 1) & (pl.col('price_change') < 0)).then(1).when((pl.col('side') == -1) & (pl.col('price_change') > 0)).then(-1).otherwise(0)
            ).alias('is_filled')
        ])
        # 4. 计算收益 (只计算成交的单子)
        # 收益 = 方向 * (未来价格变动) - Maker手续费
        # 注意：这里没有了 spread_cost！
        target_col = f"target_{lag}_tick"
        df_bt = df_bt.with_columns([
            (pl.col('side') * pl.col('is_filled') * pl.col(target_col) / 10000).alias('pnl_raw')
        ]).with_columns([
            (pl.col('pnl_raw') - pl.col('is_filled') * self.maker_fee).alias('pnl_net')
        ])

        # 5. 统计
        total_return = df_bt['pnl_net'].sum()
        fill_count = df_bt['is_filled'].abs().sum()
        attempt_count = df_bt.filter(pl.col('side') != 0).height
        fill_rate = fill_count / attempt_count if attempt_count > 0 else 0

        print(f"--- Maker 回测结果 (Th: {threshold}) ---")
        print(f"累计净收益: {total_return:.4%}")
        print(f"尝试挂单次数: {attempt_count}")
        print(f"成功成交次数: {fill_count} (成交率: {fill_rate:.2%})")
        return df_bt
    
    def find_best_maker_threshold(self, df: pl.DataFrame,weights:dict):
        results = []
        # 扫描从 1.5 到 6.0 的阈值
        for th in np.arange(1.5, 6.5, 0.5):
            bt = self.backtest(df, weights, threshold=th)
            
            # 记录核心指标
            trades = bt.filter(pl.col('is_filled') == 1)
            if trades.height > 0:
                avg_alpha = trades['pnl_raw'].mean() * 10000 # 转为 bp
                total_pnl = bt['pnl_net'].sum()
                results.append({
                    "threshold": th,
                    "total_pnl": total_pnl,
                    "avg_alpha": avg_alpha,
                    "fill_rate": trades.height / bt.filter(pl.col('side') != 0).height
                })
        
        return pl.DataFrame(results)