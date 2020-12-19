import asyncio

import pytest

from aiobitmex.http import BitmexHTTP

try:
    with open('tests/keys.txt', 'r') as f:
        API_KEY = f.readline()[:24]
        API_SECRET = f.readline()[:48]
except FileNotFoundError:
    raise Exception('API key and API secret were not found in tests/keys.txt')


# Redefine pytest-asyncio fixture with changing scope from 'function' to 'session'
@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='session')
async def test_http_conn():
    test_conn = BitmexHTTP(
        base_url='https://testnet.bitmex.com/api/v1',
        symbol='XBTUSD',
        api_key=API_KEY,
        api_secret=API_SECRET
    )
    yield test_conn
    await test_conn.session.close()
