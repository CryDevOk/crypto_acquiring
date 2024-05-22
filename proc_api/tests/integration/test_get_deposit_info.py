import pytest
import httpx

from support_functions import Customer
from dotenv import dotenv_values

config = dotenv_values("../../../.env_proc_api")

customer = Customer.get_from_file()


@pytest.mark.parametrize("input_data, expected_status_code, expected_response", [
    ({"customer_id": customer.customer_id, "user_id": "test1"}, 200, {}),
])
def test_behavior(input_data, expected_status_code, expected_response):
    response = httpx.get(f"http://localhost:{config['PROC_PORT']}/v1/api/private/user/get_deposit_info", params=input_data,
                          headers={"Content-Type": "application/json", "Api-Key": customer.api_key})

    assert response.status_code == expected_status_code
    if response.status_code == 200:
        data = response.json()
        print(data)
        assert isinstance(data, dict)
        for v in data.values():
            assert isinstance(v, dict)
            assert "address" in v and isinstance(v["address"], str)
            assert "display_name" in v and isinstance(v["display_name"], str)
            assert "coins" in v and isinstance(v["coins"], dict)
            assert 'native' in v["coins"]
            for v_ in v["coins"].values():
                assert "name" in v_ and isinstance(v_["name"], str)
                assert "min_amount" in v_ and isinstance(v_["min_amount"], str)
                assert "is_active" in v_ and isinstance(v_["is_active"], bool)


