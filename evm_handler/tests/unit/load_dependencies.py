from dotenv import dotenv_values
import os
import sys

pytest_plugins = ('pytest_asyncio',)

config_ = dotenv_values("../../../.env_proc_eth_sepolia")

for k, v in config_.items():
    os.environ[k] = v
os.environ['APP_PATH'] = './'

sys.path.append('../../')
