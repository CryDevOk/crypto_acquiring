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


def append_to_json(file_path, key_id, json_data):
    data = {}
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError:
                pass

    data[key_id] = json_data

    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)


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


class Deposit:
    def __init__(self, coin: str, amount: float, txid: str):
        self.coin = coin
        self.amount = amount
        self.txid = txid

    def to_dict(self):
        return {
            "coin": self.coin,
            "amount": self.amount,
            "txid": self.txid
        }

    @classmethod
    def load_from_dict(cls, data):
        deposit = cls(data["coin"], data["amount"], data["txid"])
        return deposit

    def __str__(self):
        return f"""
coin: {self.coin}
amount: {self.amount}
txid: {self.txid}
"""


class Withdrawal:
    def __init__(self, coin: str, amount: str, address: str):
        self.coin = coin
        self.amount = amount
        self.address = address

    def to_dict(self):
        return {
            "coin": self.coin,
            "amount": self.amount,
            "address": self.address
        }

    @classmethod
    def load_from_dict(cls, data):
        withdrawal = cls(data["coin"], data["amount"], data["address"])
        return withdrawal

    def __str__(self):
        return f"""
coin: {self.coin}
amount: {self.amount}
address: {self.address}
"""


class Account:
    def __init__(self, user_id: str, deposit_info: DepositInfo | None = None):
        self.user_id = user_id
        self.deposit_info = deposit_info
        self.deposits = []
        self.withdrawals = []

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "deposit_info": self.deposit_info.to_dict() if self.deposit_info else {},
            "deposits": [deposit.to_dict() for deposit in self.deposits],
            "withdrawals": [withdrawal.to_dict() for withdrawal in self.withdrawals]
        }

    def save_to_file(self):
        account_data = self.to_dict()
        save_json(f"{self.__class__.__name__}.json", account_data)
        print(f"Account: {self} created successfully")

    def append_to_file(self, fname=None):
        account_data = self.to_dict()
        append_to_json(f"{self.__class__.__name__}s.json" if not fname else fname, self.user_id, account_data)
        print(f"Account: {self} appended successfully")

    @classmethod
    def get_from_file(cls, fname=None):
        data = load_json(f"{Account.__name__}.json" if not fname else fname)
        return Account.load_from_dict(data)

    @classmethod
    def get_accounts_from_file(cls, fname=None):
        data = load_json(f"{Account.__name__}s.json" if not fname else fname)
        accounts = []
        for data in data.values():
            account = Account.load_from_dict(data)
            accounts.append(account)
        return accounts

    @classmethod
    def load_from_dict(cls, data):
        deposit_info = DepositInfo.load_from_dict(data["deposit_info"]) if data.get("deposit_info") else None
        account = cls(data["user_id"], deposit_info)
        account.deposits = [Deposit.load_from_dict(deposit) for deposit in data.get("deposits", [])]
        account.withdrawals = [Withdrawal.load_from_dict(withdrawal) for withdrawal in data.get("withdrawals", [])]
        return account

    def __str__(self):
        return f"""
user_id: {self.user_id}
deposit_info: {self.deposit_info}
deposits: {self.deposits}
withdrawals: {self.withdrawals}
"""
