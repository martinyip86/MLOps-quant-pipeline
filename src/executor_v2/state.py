import time


class State:
    def __init__(self,symbols:list):
        self.symbols = symbols
        self.account = {
            "asset":None,                # 账户结算资产，当前固定使用 USDT
            "free_usdt":0.0,            # CCXT 统一口径的可用 USDT，通常接近 available_balance
            "used_usdt":0.0,            # CCXT 统一口径的已占用 USDT，不等同于单个仓位保证金
            "total_usdt":0.0,           # CCXT 统一口径的 USDT 总额
            "wallet_balance":0.0,       # 钱包余额：包含已实现盈亏，不包含持仓浮动盈亏
            "available_balance":0.0,    # 当前可用于开仓/转出的余额，会受保证金占用影响
            "unrealized_pnl":0.0,       # 所有未平仓仓位的实时浮动盈亏合计
            "margin_balance":0.0,       # 账户权益，通常为 wallet_balance + unrealized_pnl
            "updated_at":None,          # 最近一次账户快照写入 State 的本地毫秒时间戳
        }

        self.position = {
            symbol:{
                "side":None,
                "contracts":0.0,
                "contract_size":0.0,
                "entry_price":0.0,
                "mark_price":0.0,
                "break_even_price":0.0,
                "notional":0.0,
                "unrealied_pnl":0.0,
                "initial_margin":0.0,
                "maintenance_margin":0.0,
                "liquidation_price":0.0,
                "margin_mode":None,
                "timestamp":None,
            }
            for symbol in self.symbols
        }

        self.account_risk = {
            # 以下六项属于 prop 套餐规则/每日基准，不能用实时账户快照覆盖。
            "initial_balance":100_000.0,          # 购买的账户初始规模
            "daily_reference_balance":100_000.0,  # 每天规则重算时刻记录的 wallet balance
            "daily_loss_limit_pct":0.03,          # 套餐允许的每日最大亏损比例
            "daily_equity_floor":97_000.0,        # 当天最低允许 equity
            "risk_reset_at":None,                 # 最近一次更新每日风控基准的时间
            "max_drawdown_floor":94_000.0,        # 套餐规定的静态最大回撤线

            # 以下三项由 update_account_data 使用最新账户快照持续覆盖。
            "balance":100_000.0,                  # 对应 wallet_balance，不含浮盈浮亏
            "equity":100_000.0,                   # 对应 margin_balance，包含全部浮盈浮亏
            "total_unrealized_pnl":0.0,           # 全账户未实现盈亏

            # 账户快照没有可靠的累计已实现盈亏字段，应在成交确认后根据成交记录更新。
            "total_realized_pnl":0.0,
            "can_trade":True,                     # 账户级开仓总开关
            "halt_reason":None,                   # 禁止开仓时记录触发原因
        }

        self.symbol_risk = {
            symbol:{
                "cooldown_until":0,
                "stop_loss_count":0,
                "last_stop_loss_time":None,
            }
            for symbol in self.symbols
        }

        self.features = {
            symbol:{}
            for symbol in self.symbols
        }

        self.snapshot = {
            symbol:{}
            for symbol in self.symbols
        }

        self.meta = {
            symbol:{
                "updated_at":None,
                "feature_ts":None,
                "spot_orderbook_ts":None,
                "swap_orderbook_ts":None,
            }
            for symbol in self.symbols
        }

    def update_account_data(self,data:dict[str,dict]):
        balance = data["balance"]
        position = data["position"]

        for key,val in balance.items():
            if key in self.account:
                self.account[key] = val

        # Exchange 返回的是全账户数据。Breakout 等 prop 风控按账户 equity
        # 判断，而不是按单个 symbol 的已实现盈亏判断，因此在这里同步到账户级风控。
        self.account["updated_at"] = int(time.time() * 1000)
        self.account_risk["balance"] = self.account["wallet_balance"]
        self.account_risk["equity"] = self.account["margin_balance"]
        self.account_risk["total_unrealized_pnl"] = self.account["unrealized_pnl"]

        for key,val in position.items():
            pos = self.position[key]
            for k,v in val.items():
                if k in pos:
                    pos[k] = v

        return self
    
    def update_market_data(self,symbol:str,features:dict,snapshot:dict):
        features_state = self.features[symbol]
        snapshot_state = self.snapshot[symbol]
        meta_state = self.meta[symbol]

        features_state.update(features)
        snapshot_state.update(snapshot)

        meta_state["updated_at"] = int(time.time() * 1000)

        if snapshot_state.get("spot_orderbook"):
            meta_state["spot_orderbook_ts"] = snapshot_state["spot_orderbook"]["timestamp"]

        if snapshot_state.get("swap_orderbook"):
            meta_state["swap_orderbook_ts"] = snapshot_state["swap_orderbook"]["timestamp"]

        meta_state["features_ts"] = max(ts for ts in [meta_state["spot_orderbook_ts"],meta_state["swap_orderbook_ts"]] if ts is not None)
    
    def get_account(self) -> dict:
        return self.account
    
    def get_features(self,symbol:str) -> dict:
        return self.features[symbol]
    
    def get_position(self,symbol:str) -> dict:
        return self.position[symbol]
    
    def get_risk(self,symbol:str) -> dict:
        return {
            "account":self.account_risk,
            symbol:self.symbol_risk[symbol]
        }
    
    def is_data_fresh(self,symbol:str,max_lag_ms:int=3000) -> bool:
        now_ms = int(time.time() * 1000)
        features_ts = self.meta[symbol]["features_ts"]

        if features_ts is None: return False

        return now_ms - features_ts <= max_lag_ms
