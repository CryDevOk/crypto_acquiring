from sqlalchemy import Column, Integer, String, ForeignKey, Identity, NUMERIC, BOOLEAN, func, event, DDL, \
    text, LargeBinary, UniqueConstraint, BIGINT, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from Crypto.Cipher import AES
from datetime import datetime
import os
from sqlalchemy.types import TypeDecorator

from config import Config as Cfg

Base = declarative_base()


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


class Users(Base):
    __tablename__ = 'users'
    id = Column(String(36), primary_key=True)
    role = Column(Integer, nullable=False)

    _user_withdrawal = relationship("Withdrawals", back_populates="user", cascade="all, delete-orphan")


class Blocks(Base):
    __tablename__ = 'blocks'
    id = Column(Integer, primary_key=True)
    deposit_count = Column(Integer, nullable=False, server_default='0')
    withdrawal_count = Column(Integer, nullable=False, server_default='0')


class Coins(Base):
    __tablename__ = 'coins'
    contract_address = Column(String(42), primary_key=True)  # contract address or "native"
    name = Column(String(16), nullable=False)
    decimal = Column(Integer, nullable=False)
    min_amount = Column(NUMERIC(36, 18), nullable=False)
    fee_amount = Column(NUMERIC(36, 18), nullable=False)
    current_rate = Column(NUMERIC(36, 18), default=None)
    is_active = Column(BOOLEAN, default=True)

    _coin_deposit = relationship("Deposits", back_populates="coin", cascade="all, delete-orphan")
    _coin_withdrawal = relationship("Withdrawals", back_populates="coin", cascade="all, delete-orphan")
    _coin_balance = relationship("Balances", back_populates="coin", cascade="all, delete-orphan")


class UserAddress(Base):
    __tablename__ = 'user_address'
    id = Column(Integer, Identity(start=1, cycle=True), primary_key=True)
    user_id = Column(String(36), ForeignKey('users.id', ondelete='CASCADE'), nullable=False, unique=True)
    admin_id = Column(String(36), ForeignKey('users.id', ondelete='CASCADE'), nullable=True, unique=False)
    approve_id = Column(String(36), ForeignKey('users.id', ondelete='CASCADE'), nullable=True, unique=False)

    public = Column(String(42), nullable=False, unique=True)
    private = Column(EncryptedData(128), nullable=False, unique=True)

    locked_by_tx = Column(BOOLEAN, default=False)

    user = relationship("Users", backref='_user_address', foreign_keys=[user_id])
    admin = relationship("Users", backref='_admin_address', foreign_keys=[admin_id])
    approve = relationship("Users", backref='_approve_address', foreign_keys=[approve_id])

    _user_deposit = relationship("Deposits", back_populates="address", cascade="all, delete-orphan")
    _admin_withdrawal = relationship("Withdrawals", back_populates="admin_addr", cascade="all, delete-orphan")
    _user_address = relationship("Balances", back_populates="address", cascade="all, delete-orphan")


class Balances(Base):
    # estimated balance of user in coin
    __tablename__ = 'balances'
    id = Column(Integer, primary_key=True)
    address_id = Column(Integer, ForeignKey('user_address.id', ondelete='CASCADE'), nullable=False, unique=False)
    coin_id = Column(String(42), ForeignKey('coins.contract_address', ondelete='CASCADE'), nullable=False, unique=False)

    balance = Column(NUMERIC(36, 18), nullable=False, default=0)

    address = relationship("UserAddress", back_populates="_user_address")
    coin = relationship("Coins", back_populates="_coin_balance")

    __table_args__ = (UniqueConstraint('address_id', 'coin_id'),)


class Deposits(Base):
    __tablename__ = "deposits"
    id = Column(String(36), primary_key=True, server_default=text("uuid_generate_v4()"))

    address_id = Column(Integer, ForeignKey('user_address.id', ondelete='CASCADE'), nullable=False, unique=False)
    address = relationship("UserAddress", back_populates="_user_deposit")

    contract_address = Column(String(42), ForeignKey('coins.contract_address', ondelete='CASCADE'), nullable=False,
                              unique=False)
    coin = relationship("Coins", back_populates="_coin_deposit")

    time_to_callback = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.fromtimestamp(0))
    callback_period = Column(Integer, nullable=False, default=60)
    locked_by_callback = Column(BOOLEAN, default=False)
    is_notified = Column(BOOLEAN, default=False)

    time_to_tx_handler = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.fromtimestamp(0))
    tx_handler_period = Column(Integer, nullable=False, default=60)
    locked_by_tx_handler = Column(BOOLEAN,
                                  default=False)  # показывает что этот депозит выполняется обработчиком транзакций
    tx_hash_out = Column(String(66), nullable=True, unique=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    tx_hash_in = Column(String(66), nullable=False, unique=True)
    amount = Column(NUMERIC(36, 18), nullable=False)
    quote_amount = Column(NUMERIC(36, 18), nullable=False)


class Withdrawals(Base):
    __tablename__ = "withdrawals"
    id = Column(String(36), primary_key=True, server_default=text("uuid_generate_v4()"))

    user_id = Column(String(36), ForeignKey('users.id', ondelete='CASCADE'), nullable=False, unique=False)
    user = relationship("Users", back_populates="_user_withdrawal")

    contract_address = Column(String(42), ForeignKey('coins.contract_address', ondelete='CASCADE'), nullable=False,
                              unique=False)
    coin = relationship("Coins", back_populates="_coin_withdrawal")

    time_to_callback = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.fromtimestamp(0))
    callback_period = Column(Integer, nullable=False, default=60)
    locked_by_callback = Column(BOOLEAN, default=False)
    is_notified = Column(BOOLEAN, default=False)

    time_to_tx_handler = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.fromtimestamp(0))
    tx_handler_period = Column(Integer, nullable=False, default=60)

    admin_addr_id = Column(Integer, ForeignKey('user_address.id', ondelete='CASCADE'), nullable=True,
                           unique=False)  # админский адрес на время обработки
    admin_addr = relationship("UserAddress", back_populates="_admin_withdrawal")

    tx_hash_out = Column(String(66), nullable=True, unique=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    withdrawal_address = Column(String(42), nullable=False)
    amount = Column(NUMERIC(36, 18), nullable=False)
    quote_amount = Column(BIGINT, nullable=False)
    user_currency = Column(String(16), nullable=False)


ddl_stmt3 = DDL("""
CREATE OR REPLACE function update_address_lock_withdrawal() RETURNS trigger AS $$
BEGIN
  UPDATE user_address AS adm SET locked_by_tx = CASE WHEN NEW.admin_addr_id IS null THEN false ELSE true END WHERE adm.id = COALESCE(NEW.admin_addr_id, OLD.admin_addr_id);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql; 
""")

ddl_stmt4 = DDL("""     
CREATE OR REPLACE TRIGGER on_update_withdrawals
  AFTER UPDATE of admin_addr_id
  ON withdrawals
  FOR EACH ROW
  EXECUTE PROCEDURE update_address_lock_withdrawal();
""")



@event.listens_for(Withdrawals.__table__, "after_create")
def my_func2(target, connection, **kw):
    connection.execute(ddl_stmt3)
    connection.execute(ddl_stmt4)
