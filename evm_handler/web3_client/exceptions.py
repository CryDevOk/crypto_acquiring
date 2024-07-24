# -*- coding: utf-8 -*-
from web3.exceptions import (
    InvalidAddress,
    Web3Exception,
    TransactionNotFound,
    ProviderConnectionError
)


class ProviderConnectionErrorOnTx(ProviderConnectionError):
    def __init__(self, tx_hash):
        self.tx_hash = tx_hash


class StuckTransaction(Web3Exception):
    def __init__(self, tx_hash, nonce):
        self.tx_hash = tx_hash
        self.nonce = nonce

    def __str__(self):
        return f"Transaction {self.tx_hash} with nonce {self.nonce} is stuck"


class AlreadyKnown(Web3Exception):
    def __init__(self, nonce):
        self.nonce = nonce


class UnderpricedTransaction(Web3Exception):
    def __init__(self, nonce):
        self.nonce = nonce


class InsufficientFundsForTx(Web3Exception):
    def __init__(self, address):
        self.address = address


class TransactionFailed(Web3Exception):
    def __init__(self, tx_hash):
        self.tx_hash = tx_hash
