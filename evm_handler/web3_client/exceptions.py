# -*- coding: utf-8 -*-
from web3.exceptions import (
    InvalidAddress,
    Web3Exception,
    TransactionNotFound
)


class StuckTransaction(Web3Exception):
    def __init__(self, tx_hash, nonce):
        self.tx_hash = tx_hash
        self.nonce = nonce


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
