import time

import pytest

from aiobitmex.auth import generate_expires, generate_signature, generate_auth_headers


@pytest.mark.parametrize(
    'offset', [None, 5]
)
def test_generate_expires(offset):
    expires = generate_expires(offset)
    now = time.time()
    assert expires > now


@pytest.mark.parametrize(
    'offset', [0, -1]
)
def test_generate_expires_with_wrong_offset(offset):
    with pytest.raises(ValueError):
        generate_expires(offset)
