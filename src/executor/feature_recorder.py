import json
import time
from logging import Logger

class FeatureRecorder:
    def __init__(self,logger:Logger):
        self.logger = logger

    def record(self,symbol:str,features:dict,snapshot:dict):
        self.logger.info(
            "[FEATURE] " + json.dumps(
                {
                    "ts":int(time.time() * 1000),
                    "symbol":symbol,
                    "features":features,
                    "snapshot":snapshot,
                },
                ensure_ascii=False
            )
        )