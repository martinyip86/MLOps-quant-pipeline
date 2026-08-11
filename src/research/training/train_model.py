from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    roc_auc_score,
    average_precision_score,
    confusion_matrix,
    precision_score,
    recall_score
)

import polars as pl

class TrainModel:
    def __init__(self):
        self.features = [
            "past_range_bps_5m",
            "realized_vol_bps_1m",
            "realized_vol_bps_5m",
            "swap_ob_ofi_1s",
            "swap_trade_flow_1s",
            "swap_obi_l1",
            "swap_obi_l5",
            "spread_swap",
        ]

    def training(self,df:pl.DataFrame):
        labeld_df = df.sort("entry_timestamp")

        split_index = int(labeld_df.height * 0.8)
        split_timestamp = labeld_df["entry_timestamp"][split_index]

        purge_ms = 5 * 60_000

        quiet_market = (pl.col("past_range_bps_5m") < 12)

        train_df = labeld_df.filter((pl.col("entry_timestamp") < split_timestamp - purge_ms) & quiet_market)

        test_df = labeld_df.filter((pl.col("entry_timestamp") >= split_timestamp) & quiet_market)

        X_train = train_df.select(self.features).to_numpy()
        y_train = train_df.get_column("volatility_trigger").to_numpy()

        X_test = test_df.select(self.features).to_numpy()
        y_test = test_df.get_column("volatility_trigger").to_numpy()

        model = Pipeline([
            ("scaler",StandardScaler()),
            ("classifier",LogisticRegression(max_iter=1000))
        ])

        model.fit(X_train,y_train)

        train_probability = model.predict_proba(X_train)[:,1]

        test_probability = model.predict_proba(X_test)[:,1]

        test_prediction = (test_probability >= 0.5).astype(int)

        print("train rows: ",train_df.height)
        print("test rows: ",test_df.height)

        print("train positive ratio: ",y_train.mean())
        print("test positive ratio: ",y_test.mean())

        print("train ROC-AUC: ",roc_auc_score(y_train,train_probability))
        print("test ROC-AUC: ",roc_auc_score(y_test,test_probability))

        print("test PR-AUC: ",average_precision_score(y_test,test_probability))

        print("test precision: ",precision_score(y_test,test_prediction,zero_division=0))

        print("test recall: ",recall_score(y_test,test_prediction,zero_division=0))

        print("confusion matrix")
        print(confusion_matrix(y_test,test_prediction))

        return model
