import random

import pytest
import httpx
import uuid
from support_functions import Customer, Account, Withdrawal
from dotenv import dotenv_values

config_proc_api = dotenv_values("../../../.env_proc_api")
config_tron = dotenv_values("../../../.env_proc_tron_nile")
print(config_tron)

customer = Customer.get_from_file()
accounts = Account.get_accounts_from_file()
contract_address = config_tron['TEST_CONTRACT_ADDRESS']
address = config_tron['TEST_ADDRESS']


def withdraw(account):
    contract_address_ = random.choice([contract_address, "native"])
    amount = str(random.randint(1, 10))
    response = httpx.post(f"http://localhost:{config_proc_api['PROC_PORT']}/v1/api/private/user/create_withdrawal",
                          json={
                              "customer_id": customer.customer_id,
                              "user_id": account.user_id,
                              "tx_handler": "tron_nile",
                              "contract_address": contract_address_,
                              "address": address,
                              "quote_amount": amount,
                              "user_currency": "USD"
                          },
                          headers={"Content-Type": "application/json", "Api-Key": customer.api_key})

    assert response.status_code == 200
    data = response.json()
    print(data)
    account.withdrawals.append(Withdrawal(contract_address_, amount, address))


if __name__ == "__main__":
    for account in accounts:
        withdraw(account)
        account.append_to_file()