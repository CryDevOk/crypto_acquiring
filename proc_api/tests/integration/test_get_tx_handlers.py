import pytest
import httpx

from support_functions import Customer
from dotenv import dotenv_values

config = dotenv_values("../../../.env_proc_api")

customer = Customer.get_from_file()


@pytest.mark.parametrize("input_data, expected_status_code, expected_response", [
    ({"customer_id": customer.customer_id}, 200, {}),
])
def test_behavior(input_data, expected_status_code, expected_response):
    response = httpx.get(f"http://localhost:{config['PROC_PORT']}/v1/api/private/get_tx_handlers", params=input_data,
                          headers={"Content-Type": "application/json", "Api-Key": customer.api_key})

    assert response.status_code == expected_status_code
    if response.status_code == 200:
        data = response.json()
        assert isinstance(data, dict)
        for v in data.values():
            assert isinstance(v, dict)
            assert "display_name" in v
            assert "name" in v
