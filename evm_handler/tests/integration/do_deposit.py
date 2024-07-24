import web3
from support_functions import Account, Deposit, erc20_abi
from dotenv import dotenv_values

config_proc_api = dotenv_values("../../../.env_proc_api")
config_eth = dotenv_values("../../../.env_proc_eth_sepolia")
print(config_eth)

provider_url = config_eth['PROC_HANDLER_PROVIDER_INFURA_URL']
api_key = config_eth['PROC_HANDLER_INFURA_API_KEYS'].split(",")[0]
provider_url = provider_url.rstrip("/") + "/" + api_key

private_key = config_eth['TEST_PRIVATE_KEY']
contract_address = config_eth['TEST_CONTRACT_ADDRESS']
chain_id = int(config_eth['PROC_HANDLER_NETWORK_ID'])
# account = Account.get_from_file()
accounts = Account.get_accounts_from_file()


def eth_deposit(account, amount):
    w3 = web3.Web3(web3.HTTPProvider(provider_url))
    priv_key = w3.eth.account.from_key(private_key)
    gas_price = int(w3.eth.gas_price * 1.5)
    txn = {
        "to": account.deposit_info.address,
        "value": amount,
        "gas": 200_000,
        "gasPrice": gas_price,
        "nonce": w3.eth.get_transaction_count(priv_key.address),
        "chainId": chain_id,
    }
    signed_txn = w3.eth.account.sign_transaction(txn, private_key)
    txn_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
    print(txn_hash.hex())
    w3.eth.wait_for_transaction_receipt(txn_hash)
    account.deposits.append(Deposit("native", amount, txn_hash.hex()))


def coin_deposit(account, amount):
    w3 = web3.Web3(web3.HTTPProvider(provider_url))
    priv_key = w3.eth.account.from_key(private_key)
    contract = w3.eth.contract(address=contract_address, abi=erc20_abi)
    gas_price = int(w3.eth.gas_price*1.5)
    txn = contract.functions.transfer(account.deposit_info.address, amount).build_transaction({
        "gas": 200_000,
        "gasPrice": gas_price,
        "nonce": w3.eth.get_transaction_count(priv_key.address),
        "chainId": chain_id,
    })
    signed_txn = w3.eth.account.sign_transaction(txn, private_key)
    txn_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
    print(txn_hash.hex())
    w3.eth.wait_for_transaction_receipt(txn_hash)
    account.deposits.append(Deposit(contract_address, amount, txn_hash.hex()))


if __name__ == "__main__":
    import random

    # eth_deposit(accounts[1], int(0.01*10**18))
    for account in accounts[0:3]:
        # eth_deposit(account, random.randint(int(0.0031*10**18), int(0.005*10**18)))
        coin_deposit(account, random.randint(int(3*10**6), int(5*10**6)))
        account.append_to_file()
