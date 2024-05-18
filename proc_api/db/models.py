# -*- coding: utf-8 -*-
from config import Config as Cfg

from sqlalchemy import Column, Integer, String, true, Boolean, ForeignKey, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TypeDecorator, LargeBinary
from sqlalchemy.orm import relationship
from Crypto.Cipher import AES
import os


def aes_encrypt(data: str, key: bytes) -> bytes:
    nonce = os.urandom(16)
    cipher = AES.new(key, AES.MODE_EAX, nonce=nonce)
    data = data + (" " * (16 - (len(data) % 16)))
    return cipher.encrypt(data.encode("utf-8")) + nonce


def aes_decrypt(data: bytes, key: bytes) -> str:
    cipher = AES.new(key, AES.MODE_EAX, nonce=data[-16:])
    return cipher.decrypt(data[:-16]).decode("utf-8").rstrip()


class EncryptedData(TypeDecorator):
    impl = LargeBinary
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            return aes_encrypt(value, Cfg.DB_SECRET_KEY)  # Replace 'key' with your encryption key

    def process_result_value(self, value, dialect):
        if value is not None:
            return aes_decrypt(value, Cfg.DB_SECRET_KEY)  # Replace 'key' with your encryption key


Base = declarative_base()


class Customer(Base):
    __tablename__ = 'customer'
    id = Column(String(36), primary_key=True, server_default=text("uuid_generate_v4()"))
    callback_url = Column(String(255), nullable=False, unique=True)
    callback_api_key = Column(EncryptedData(255), nullable=False)
    api_key = Column(EncryptedData(255), nullable=False, unique=True)

    user = relationship("User", back_populates="customer", cascade="all, delete-orphan")


class User(Base):
    __tablename__ = 'user'
    id = Column(String(36), primary_key=True)
    role = Column(Integer, nullable=False)

    customer_id = Column(String(36), ForeignKey('customer.id', ondelete='SET NULL'), nullable=True, unique=False)
    customer = relationship("Customer", back_populates="user")


class NetworkHandlers(Base):
    __tablename__ = 'network_handlers'
    name = Column(String(36), primary_key=True)
    display_name = Column(String(64), nullable=False)
    server_url = Column(String(255), nullable=False, unique=True)
    api_key = Column(EncryptedData(255), nullable=False)

    is_active = Column(Boolean, nullable=False, server_default=true())
