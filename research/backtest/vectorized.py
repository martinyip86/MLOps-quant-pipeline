import polars as pl
import numpy as np

class Vectorized:
    def __init__(self):
        pass

    def vectorized_backtest(self,df:pl.DataFrame,weights:dict,threshold:float=4.0):
        """
        基于你存储的 coef 和 intercept 进行回测
        """
        # 1. 重构特征 (这一步必须与训练时完全一致)
        # 假设 df 已经包含了指标计算
        
        # 2. 计算组合信号 (Z-Score)
        # 公式: (X * coef + intercept) / signal_scale
        coef = weights['coef']
        intercept = weights['intercept']
        scale = weights['signal_scale']
        df_bt = df.with_columns([
            ((pl.col('vamp_bias') * coef[0] + pl.col('ofi') * coef[1] + pl.col('imbalance') * coef[2] + intercept) / scale).alias('z_score')
        ])

        # 3. 模拟持仓 (核心：必须 shift(1) 避免前瞻偏差)
        df_bt = df_bt.with_columns([
            (pl.when(pl.col('z_score') > threshold).then(1).when(pl.col('z_score') < -threshold).then(-1).otherwise(0)).alias('raw_pos')
        ]).with_columns([
            pl.col('raw_pos').shift(1).fill_null(0).alias('position')
        ])

        # 4. 计算真实收益 (使用 Tick 级的变动)
        # 注意：这里建议使用 mid_price 的百分比变动，而不是 target_lag
        df_bt = df_bt.with_columns([
            (pl.col('position') * (pl.col('mid_price').diff() / pl.col('mid_price').shift(1))).alias('pnl_raw'),
            pl.when(pl.col('position') > 0).then(pl.col('mid_price') + pl.col('spread') / 2).when(pl.col('position') == -1).then(pl.col('mid_price') - pl.col('spread') / 2).otherwise(None).alias('fill_price')
        ])

        # 5. 扣除手续费 (每当 position 变化时扣除)
        fee_rate = 0.0002
        df_bt = df_bt.with_columns([
            (pl.col('position').diff().abs() * (fee_rate + pl.col('spread') / pl.col('mid_price'))).fill_null(0).alias('cost')
        ]).with_columns([
            (pl.col('pnl_raw') - pl.col('cost')).alias('pl_net')
        ])
        df_bucket = df_bt.with_columns([
            (pl.col("z_score").cut([-10,-5,-3,-2,2,3,5,10])).alias("bucket")
        ]).group_by("bucket").agg([
            pl.col("pnl_raw").mean()
        ])
        win_rate = np.mean((df_bucket["pnl_raw"].to_numpy() > 0))
        print(f"win_rate: {win_rate:.4f}")
        print(f"当前模型最大信号强度: {df_bt['z_score'].abs().max()}")
        print(f"累计净收益: {df_bt['pl_net'].sum():.4%}")
        return df_bt
    
    def find_breakeven_threshold(self,df:pl.DataFrame,fee_bps=4.0):
        results = []

        for th in np.arange(4.0,10.0,0.5):
            trades = df.filter(pl.col('z_score').abs() > th)
            if len(trades) == 0: continue

            avg_alpha = trades['pnl_raw'].mean() * 10000
            net_pnl = (avg_alpha - fee_bps) * len(trades)
            fill_price = trades['fill_price']

            print(f"Th: {th:.1f} | 交易次数: {len(trades)} | 单笔Alpha: {avg_alpha:.2f}bp | 净收益: {net_pnl:.2f}")