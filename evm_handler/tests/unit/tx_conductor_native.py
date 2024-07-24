import asyncio
import pytest
from unittest.mock import AsyncMock, patch, call, MagicMock

import load_dependencies

from tx_handler import tx_conductor_native
from db.models import Coins
from config import StatCode as St


class WithAsyncContextManager:

    async def __aenter__(self, *args, **kwargs):
        return self

    async def __aexit__(self, *args, **kwargs):
        pass


@patch("sqlalchemy.ext.asyncio.async_sessionmaker")
@pytest.mark.asyncio
async def test_admin_coins_balance(mock_write_async_session):
    mock_db = AsyncMock()
    get_and_lock_pending_deposits_native = AsyncMock(return_value=[{"deposit_id": "deposit_id_1",
                                                                    "amount": 1,
                                                                    "user_private": "user_private_1",
                                                                    "admin_public": "admin_public_1",
                                                                    "tx_handler_period": 1,
                                                                    "address_id": "address_id_1",
                                                                    "tx_hash_out": None}])
    mock_db.get_and_lock_pending_deposits_native = get_and_lock_pending_deposits_native
    mock_db.update_user_address_by_id = AsyncMock()
    mock_db.update_deposit_by_id = AsyncMock()

    native_transfer_to_admin = AsyncMock(return_value=(None, None, ("deposit_id_1", 1, "address_id_1", None)))

    with patch('tx_handler.write_async_session', mock_write_async_session), \
            patch('tx_handler.DB', return_value=mock_db), \
            patch('tx_handler.native_transfer_to_admin', native_transfer_to_admin):
        await tx_conductor_native()

        mock_write_async_session.assert_called_once()
        get_and_lock_pending_deposits_native.assert_called_once()


# Add more test cases to cover different scenarios, such as when there are errors or different coin configurations
