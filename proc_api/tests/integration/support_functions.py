import httpx
import os
import random
import json


def load_json(file_path):
    # Check if the file exists
    if os.path.exists(file_path):
        # File exists, so read its contents
        with open(file_path, 'r') as file:
            try:
                json_data = json.load(file)
            except json.JSONDecodeError:
                json_data = None
    else:
        json_data = None
    return json_data


def save_json(file_path, json_data):
    with open(file_path, 'w') as file:
        json.dump(json_data, file, indent=4)


class Customer:
    def __init__(self):
        self.api_key = None
        self.customer_id = None

    def to_dict(self):
        return {
            "api_key": self.api_key,
            "customer_id": self.customer_id
        }

    @classmethod
    def load_from_dict(cls, data):
        user = cls()
        user.api_key = data["api_key"]
        user.customer_id = data["customer_id"]
        return user

    def __str__(self):
        return f"""
api_key: {self.api_key}
customer_id: {self.customer_id}
"""

    @classmethod
    def get_from_file(cls):
        data = load_json(f"{Customer.__name__}.json")
        return Customer.load_from_dict(data)

    def save_to_file(self, user_info):
        user_data = self.to_dict()
        save_json(f"{user_info.__class__.__name__}.json", user_data)
        print(f"User email: {user_info} signed up successfully")


class DepositInfo:
    def __init__(self):
        self.address = None
        self.display_name = None
        self.coins = None

    def to_dict(self):
        return {
            "address": self.address,
            "display_name": self.display_name,
            "coins": self.coins
        }

    @classmethod
    def load_from_dict(cls, data):
        deposit_info = cls()
        deposit_info.address = data["address"]
        deposit_info.display_name = data["display_name"]
        deposit_info.coins = data["coins"]
        return deposit_info

    def __str__(self):
        return f"""
address: {self.address}
display_name: {self.display_name}
coins: {self.coins}
"""


class Account:
    def __init__(self, user_id: str, deposit_info: DepositInfo | None = None):
        self.user_id = user_id
        self.deposit_info = deposit_info

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "deposit_info": self.deposit_info.to_dict() if self.deposit_info else {}
        }

    def save_to_file(self, account):
        account_data = self.to_dict()
        save_json(f"{account.__class__.__name__}.json", account_data)
        print(f"Account: {account} created successfully")

    @classmethod
    def get_from_file(cls):
        data = load_json(f"{Account.__name__}.json")
        return Account.load_from_dict(data)

    @classmethod
    def load_from_dict(cls, data):
        deposit_info = DepositInfo.load_from_dict(data["deposit_info"]) if data.get("deposit_info") else None
        account = cls(data["user_id"], deposit_info)
        return account

    def __str__(self):
        return f"""
user_id: {self.user_id}
deposit_info: {self.deposit_info}
"""
