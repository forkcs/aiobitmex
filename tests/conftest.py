import pytest

from aiobitmex.http import BitmexHTTP


@pytest.fixture
async def test_http_conn():
    test_conn = BitmexHTTP(base_url='https://testnet.bitmex.com/api/v1', symbol='XBTUSD', api_key='', api_secret='')
    yield test_conn
    await test_conn.session.close()
