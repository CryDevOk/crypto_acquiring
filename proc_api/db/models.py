# -*- coding: utf-8 -*-
from config import Config as Cfg

from sqlalchemy import Column, Integer, String, true, Boolean, ForeignKey, text, false, JSON, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TypeDecorator, LargeBinary
from sqlalchemy.orm import relationship
from Crypto.Cipher import AES
from hashlib import sha256
import os
from datetime import datetime


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


class HashedData(TypeDecorator):
    impl = LargeBinary
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            return sha256(Cfg.DB_SECRET_KEY + value.encode("utf-8")).digest()


Base = declarative_base()


class Customer(Base):
    __tablename__ = 'customer'
    id = Column(String(36), primary_key=True, server_default=text("uuid_generate_v4()"))
    callback_url = Column(String(255), nullable=False, unique=True)
    callback_api_key = Column(EncryptedData(255), nullable=False)
    api_key = Column(HashedData(32), nullable=False, unique=True)

    user = relationship("User", back_populates="customer", cascade="all, delete-orphan")


class User(Base):
    __tablename__ = 'user'
    id = Column(String(36), primary_key=True)
    role = Column(Integer, nullable=False)

    customer_id = Column(String(36), ForeignKey('customer.id', ondelete='SET NULL'), nullable=True, unique=False)
    customer = relationship("Customer", back_populates="user")

    callbacks = relationship("Callbacks", back_populates="user", cascade="all, delete-orphan")


class Callbacks(Base):
    __tablename__ = 'callbacks'
    id = Column(String(64), primary_key=True)
    user_id = Column(String(36), ForeignKey('user.id', ondelete='CASCADE'), nullable=False, unique=False)
    path = Column(String(255), nullable=False)
    json_data = Column(JSON, nullable=False)
    is_notified = Column(Boolean, nullable=False, server_default=false())

    locked_by_callback = Column(Boolean, nullable=False, server_default=false())
    time_to_callback = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.fromtimestamp(0))
    callback_period = Column(Integer, nullable=False, default=60)

    user = relationship("User", back_populates="callbacks")


class NetworkHandlers(Base):
    __tablename__ = 'network_handlers'
    name = Column(String(36), primary_key=True)
    display_name = Column(String(64), nullable=False)
    server_url = Column(String(255), nullable=False, unique=True)
    api_key = Column(EncryptedData(255), nullable=False)

    is_active = Column(Boolean, nullable=False, server_default=true())
