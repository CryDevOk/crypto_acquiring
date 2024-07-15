from tronpy import Tron
from tronpy.providers import HTTPProvider
from tronpy.keys import PrivateKey
from support_functions import Account
from dotenv import dotenv_values

config_proc_api = dotenv_values("../../../.env_proc_api")
config_tron = dotenv_values("../../../.env_proc_tron_nile")
print(config_tron)

provider_url = config_tron['PROC_HANDLER_PROVIDER_TRONGRID_URL']
api_key = config_tron['PROC_HANDLER_TRONGRID_API_KEYS'].split(",")[0]
private_key = config_tron['TEST_PRIVATE_KEY']

account = Account.get_from_file()


def trx_deposit():
    client = Tron(HTTPProvider(provider_url, api_key=api_key))

    priv_key = PrivateKey(bytes.fromhex(private_key))

    txn = (
        client.trx.transfer(priv_key.public_key.to_base58check_address(), account.deposit_info.address, 5_000_000)
        .build()
        .sign(priv_key)
    )
    print(txn.txid)
    print(txn.broadcast().wait())


if __name__ == "__main__":
    trx_deposit()
