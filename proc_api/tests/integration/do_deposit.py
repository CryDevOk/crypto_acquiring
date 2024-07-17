from tronpy import Tron
from tronpy.providers import HTTPProvider
from tronpy.keys import PrivateKey
from support_functions import Account, Deposit
from dotenv import dotenv_values

config_proc_api = dotenv_values("../../../.env_proc_api")
config_tron = dotenv_values("../../../.env_proc_tron_nile")
print(config_tron)

provider_url = config_tron['PROC_HANDLER_PROVIDER_TRONGRID_URL']
api_key = config_tron['PROC_HANDLER_TRONGRID_API_KEYS'].split(",")[0]
private_key = config_tron['TEST_PRIVATE_KEY']
contract_address = config_tron['TEST_CONTRACT_ADDRESS']

# account = Account.get_from_file()
accounts = Account.get_accounts_from_file()


def trx_deposit(account, amount):
    tron_client = Tron(HTTPProvider(provider_url, api_key=api_key))

    priv_key = PrivateKey(bytes.fromhex(private_key))

    txn = (
        tron_client.trx.transfer(priv_key.public_key.to_base58check_address(), account.deposit_info.address, amount)
        .build()
        .sign(priv_key)
    )
    print(txn.txid)
    print(txn.broadcast().wait())
    account.deposits.append(Deposit("native", amount, txn.txid))



def coin_deposit(account, amount):
    client = Tron(HTTPProvider(provider_url, api_key=api_key))
    contract = client.get_contract(contract_address)
    priv_key = PrivateKey(bytes.fromhex(private_key))

    txn = (
        contract.functions.transfer(account.deposit_info.address, amount)
        .with_owner(priv_key.public_key.to_base58check_address())  # address of the private key
        .fee_limit(20_000_000)
        .build()
        .sign(priv_key)
    )
    print(txn.txid)
    print(txn.broadcast().wait())
    account.deposits.append(Deposit(contract_address, amount, txn.txid))


if __name__ == "__main__":
    import random
    for account in accounts[:2]:
        trx_deposit(account, random.randint(1_000_000, 10_000_000))
        coin_deposit(account, random.randint(1_000_000, 10_000_000))
        account.append_to_file()


