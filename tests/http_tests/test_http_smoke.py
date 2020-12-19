import pytest


@pytest.mark.asyncio
async def test_make_request(test_http_conn):
    response = await test_http_conn._make_request(path='/', verb='GET')
    assert response['name'] == 'BitMEX API'
