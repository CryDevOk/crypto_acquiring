import pytest
import httpx
from support_functions import Customer, Account
from dotenv import dotenv_values

config_proc_api = dotenv_values("../../../.env_proc_api")
config_tron = dotenv_values("../../../.env_proc_tron_nile")
print(config_tron)

TEST_ADDRESS = config_tron['TEST_ADDRESS']

customer = Customer.get_from_file()
account = Account.get_from_file()


@pytest.mark.parametrize("input_data, expected_status_code, expected_response", [
    ({
         "customer_id": customer.customer_id,
         "user_id": account.user_id,
         "tx_handler": "tron_nile",
         "contract_address": "native",
         "address": TEST_ADDRESS,
         "quote_amount": 3,
         "user_currency": "USD"
     }, 200, {}),
])
def test_behavior(input_data, expected_status_code, expected_response):
    response = httpx.post(f"http://localhost:{config_proc_api['PROC_PORT']}/v1/api/private/user/create_withdrawal",
                          json=input_data,
                          headers={"Content-Type": "application/json", "Api-Key": customer.api_key})

    assert response.status_code == expected_status_code
    assert response.json() == expected_response
