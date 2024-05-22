import pytest
import httpx

from support_functions import Customer
from dotenv import dotenv_values

config = dotenv_values("../../../.env_proc_api")

customer = Customer()


@pytest.mark.parametrize("input_data, expected_status_code, expected_response", [
    ({"callback_url": "http://httpbin", "callback_api_key": "test"}, 200, {}),
    # ({"callback_url": "http://localhost", "callback_api_key": "test"}, 409, {}),
])
def test_behavior(input_data, expected_status_code, expected_response):
    response = httpx.post(f"http://localhost:{config['PROC_PORT']}/v1/api/private/user/add_customer", json=input_data,
                          headers={"Content-Type": "application/json", "Api-Key": config['PROC_API_KEY']})

    assert response.status_code == expected_status_code
    if response.status_code == 200:
        data = response.json()
        assert "api_key" in data and "customer_id" in data
        customer.api_key = data["api_key"]
        customer.customer_id = data["customer_id"]
        customer.save_to_file(customer)
