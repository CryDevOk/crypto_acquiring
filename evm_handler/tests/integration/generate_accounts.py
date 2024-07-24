import httpx
import uuid
from support_functions import Customer, Account, DepositInfo
from dotenv import dotenv_values

config = dotenv_values("../../../.env_proc_api")

customer = Customer.get_from_file()


def generate_account(customer_id, user_id):
    response = httpx.post(f"http://localhost:{config['PROC_PORT']}/v1/api/private/user/add_account",
                          json={"customer_id": customer_id, "user_id": user_id},
                          headers={"Content-Type": "application/json", "Api-Key": customer.api_key})

    assert response.status_code == 200

    response = httpx.get(f"http://localhost:{config['PROC_PORT']}/v1/api/private/user/get_deposit_info",
                         params={"customer_id": customer_id, "user_id": user_id},
                         headers={"Content-Type": "application/json", "Api-Key": customer.api_key})

    data = response.json()

    account = Account(user_id)
    account.deposit_info = DepositInfo.load_from_dict(data["eth_sepolia"])
    account.append_to_file(fname=f"{Account.__name__}s.json")


if __name__ == "__main__":
    for i in range(10):
        generate_account(customer.customer_id, str(uuid.uuid4()))
