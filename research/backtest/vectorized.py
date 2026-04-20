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
        lag = weights['best_lag']
        target_col = f"target_{lag}_tick"
        df_bt = df.with_columns([
            ((pl.col('vamp_bias') * coef[0] + pl.col('ofi') * coef[1] + pl.col('imbalance') * coef[2] + intercept) / scale).alias('z_score')
        ])

        # 3. 模拟持仓 (核心：必须 shift(1) 避免前瞻偏差)
        df_bt = df_bt.with_columns([
            (pl.when(pl.col('z_score') > threshold).then(1).when(pl.col('z_score') < -threshold).then(-1).otherwise(0)).alias('raw_pos')
        ]).with_columns([
            pl.col('raw_pos').shift(1).fill_null(0).alias('position')
        ])
        # position 在 t 时刻持有 → 实际在 t+lag 实现 target_lag 的收益
        df_bt = df_bt.with_columns([
            pl.col('position').shift(lag).fill_null(0).alias('position_for_pnl')
        ]).with_columns([
            # target 是 *10000 的 bp 形式，所以要 /10000 转回小数
            (pl.col('position_for_pnl') * pl.col(target_col) / 10000).alias('pnl_raw')
        ])

        # 4. 计算真实收益 (使用 Tick 级的变动)
        # 注意：这里建议使用 mid_price 的百分比变动，而不是 target_lag
        # df_bt = df_bt.with_columns([
        #     (pl.col('position') * (pl.col('mid_price').diff() / pl.col('mid_price').shift(1))).alias('pnl_raw'),
        #     pl.when(pl.col('position') > 0).then(pl.col('mid_price') + pl.col('spread') / 2).when(pl.col('position') == -1).then(pl.col('mid_price') - pl.col('spread') / 2).otherwise(None).alias('fill_price')
        # ])

        # 5. 扣除手续费 (每当 position 变化时扣除)
        fee_rate = 0.0004
        spread_cost = 2 / 10000
        df_bt = df_bt.with_columns([
            (pl.col('position_for_pnl').diff().abs() * (fee_rate + spread_cost)).fill_null(0).alias('cost')
        ]).with_columns([
            (pl.col('pnl_raw') - pl.col('cost')).alias('pl_net')
        ])
        total_return = df_bt['pl_net'].sum()
        trades = df_bt.filter(pl.col('position_for_pnl').diff().abs() > 0)
        num_trades = len(trades) // 2
        win_rate = (df_bt.filter(pl.col('pnl_raw') > 0)['pnl_raw'].count() / df_bt.filter(pl.col('pnl_raw') != 0)['pnl_raw'].count()) if len(trades) > 0 else 0.0
        if len(df_bt) > 0:
            avg_pnl_bp = df_bt.filter(pl.col('pnl_raw') != 0)['pnl_raw'].mean() * 10000
        else:
            avg_pnl_bp = 0
        print(f"✅ 【第2版】回测（lag={lag}, threshold={threshold}）")
        print(f"累计净收益: {total_return:.4%}")
        print(f"交易次数（round-trip）: {num_trades}")
        print(f"胜率: {win_rate:.2%}")
        print(f"单笔平均 Alpha: {avg_pnl_bp:.2f} bp")
        print(f"最大信号强度: {df_bt['z_score'].abs().max():.2f}")
        return df_bt
    
    def find_breakeven_threshold(self,df:pl.DataFrame,weights:dict,fee_bps=4.0):
        results = []

        for th in np.arange(4.0,9.0,0.5):
            bt = self.vectorized_backtest(df,weights,th)
            trades = bt.filter(pl.col('z_score').abs() > th)
            if len(trades) == 0: continue

            avg_alpha = trades['pnl_raw'].mean() * 10000
            net_pnl = (avg_alpha - fee_bps) * len(trades)

            print(f"Th: {th:.1f} | 交易次数: {len(trades)} | 单笔Alpha: {avg_alpha:.2f}bp | 净收益: {net_pnl:.2f}")