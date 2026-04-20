import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv()

class ClickhouseManager:
    def __init__(self):
        self._ch_client = None

    def connect(self,host='local'):
        if self._ch_client is None:
            host = os.getenv('CLICKHOUSE_HOST' if host == 'local' else 'HK_HOST')
            port = os.getenv('CLICKHOUSE_PORT')
            username = os.getenv('CLICKHOUSE_USERNAME')
            password = os.getenv('CLICKHOUSE_PASSWORD')
            database = os.getenv('CLICKHOUSE_DB')
            try:
                self._ch_client = clickhouse_connect.get_client(
                    host=host,
                    port=int(port),
                    username=username,
                    password=password,
                    database=database
                )
                print(f"✅ [DATABASE] ClickHouse connection established: {host}:{port}")
            except Exception as e:
                print(f"❌ [DATABASE-ERROR] Failed to connect to ClickHouse: {e}")
                raise e
            
        return self._ch_client
    
ch_manager = ClickhouseManager()