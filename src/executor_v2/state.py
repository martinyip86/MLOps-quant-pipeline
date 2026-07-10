class State:
    def __init__(self,symbols:list):
        self.symbols = symbols
        self.account = {
            "asset":None,
            "free_usdt":0.0,
            "used_usdt":0.0,
            "total_usdt":0.0,
            "wallet_balance":0.0,
            "available_balance":0.0,
            "unrealized_pnl":0.0,
            "margin_balance":0.0,
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
                "timesatmp":None,
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

    def update_account_data(self,data:dict[str,dict]):
        balance = data["balance"]
        position = data["position"]

        for key,val in balance.items():
            self.account[key] = val

        for key,val in position.items():
            pos = self.position[key]
            for k,v in pos:
                if k in pos:
                    pos[k] = v

        return self
    
    def update_market_data(self,symbol:str,features:dict,snapshot:dict):
        featurs = self.symbols[symbol]
        snapshot = self.snapshot[symbol]

        self.features.update(features)
        self.snapshot.update(snapshot)
    
    def get_account(self):
        return self.account