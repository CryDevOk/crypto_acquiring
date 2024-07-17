import pytest
import httpx
import uuid
from support_functions import Customer, Account
from dotenv import dotenv_values

config = dotenv_values("../../../.env_proc_api")

customer = Customer.get_from_file()

user_id = str(uuid.uuid4())


@pytest.mark.parametrize("input_data, expected_status_code, expected_response", [
    ({"customer_id": customer.customer_id, "user_id": user_id}, 200, {}),
])
def test_behavior(input_data, expected_status_code, expected_response):
    response = httpx.post(f"http://localhost:{config['PROC_PORT']}/v1/api/private/user/add_account", json=input_data,
                          headers={"Content-Type": "application/json", "Api-Key": customer.api_key})

    assert response.status_code == expected_status_code
    assert response.json() == expected_response

    if response.status_code == 200:
        account = Account(user_id)
        account.save_to_file()
        print(account)
