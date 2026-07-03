from src.workers.trades_spot_patcher import TradesSpotPatcher
from workers.trades_swap_patcher import TradesSwapPatcher
from workers.mark_price_swap_patcher import MarkPriceSwapPatcher
from src.utils.logger import setup_logger

from dotenv import load_dotenv
from datetime import datetime,timedelta,timezone
import paramiko
import glob
import os
import time

load_dotenv()

class DailyPatcher:
    def __init__(self,target_date:str=None):
        self.target_date = target_date
        self.exchange_ids = ['binance']
        self.symbols = ['BTC/USDT','ETH/USDT','SOL/USDT','XRP/USDT']
        self.logger = setup_logger(
            name='daily.patcher',
            log_file='logs/workers/daily_patcher.log'
        )

    def connect_ssh(self):
        for i in range(5):
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                ssh.connect(
                    hostname=os.getenv("HK_HOST"),
                    username=os.getenv("SSH_USERNAME"),
                    key_filename=os.path.expanduser("~/.ssh/id_rsa"),
                    timeout=30,
                    banner_timeout=60,
                    auth_timeout=60,
                )
                return ssh

            except Exception as e:
                self.logger.error(f"SSH connect failed retry={i+1}: {e}")
                time.sleep(2 ** i)

        raise Exception("SSH connect failed after retries")

    def upload_to_server(self,exchange_id:str,target_date:str,symbol:str):
        if target_date is None or target_date >= datetime.now(timezone.utc).strftime('%Y-%m-%d'):
            if exchange_id == 'binance':
                target_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')

        clear_symbol = symbol.replace('/','-')
        file_paths = f"data/patch/{target_date}/{clear_symbol}/*.parquet"
        file_list = glob.glob(file_paths)

        ssh = self.connect_ssh()

        sftp = ssh.open_sftp()

        uploaded_files = []

        print(file_list)

        try:
            for local_path in file_list:
                file_name = os.path.basename(local_path)
                table_name = file_name.replace(".parquet","")

                remote_path = os.path.join(
                    "/home/ubuntu/ouroboros-quant-pipeline/temp",
                    f"{clear_symbol}_{file_name}"
                )

                self.logger.info(f"⬆️ Uploading {local_path} -> {remote_path}")

                sftp.put(local_path, remote_path)

                self.logger.info(f"✅ Upload complete: {remote_path}")

                cmd = f"""
                docker exec -i clickhouse-server clickhouse-client \
                --query="INSERT INTO market_data.{table_name} SETTINGS async_insert=0 FORMAT Parquet" \
                < {remote_path}
                """

                stdin,stdout,stderr = ssh.exec_command(cmd)

                # out = stdout.read().decode()
                # err = stderr.read().decode()

                # self.logger.info(out)

                exit_code = stdout.channel.recv_exit_status()

                if exit_code != 0:
                    raise Exception(stderr.read().decode())
                
                ssh.exec_command(
                    f"rm -f {remote_path}"
                )

                uploaded_files.append(remote_path)

        except Exception as e:
            self.logger.error(f"upload error: {e}")
        finally:
            sftp.close()
            ssh.close()

        return uploaded_files

    def main(self):
        for exchange_id in self.exchange_ids:
            for symbol in self.symbols:
                TradesSpotPatcher(exchange_id,symbol,self.target_date,self.logger).main()
                time.sleep(5)
                TradesSwapPatcher(exchange_id,symbol,self.target_date,self.logger).main()
                time.sleep(5)
                MarkPriceSwapPatcher(exchange_id,symbol,self.target_date,self.logger).main()
                time.sleep(5)
                self.upload_to_server(exchange_id,self.target_date,symbol)

if __name__ == '__main__':
    obj = DailyPatcher()
    obj.main()