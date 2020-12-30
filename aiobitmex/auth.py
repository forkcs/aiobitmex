import hashlib
import hmac
import time
from urllib.parse import urlparse


def generate_expires(offset: int = None) -> int:
    if isinstance(offset, int) and offset < 1:
        raise ValueError('Offset must be a positive integer.')
    if offset is None:
        offset = 60
    return int(time.time() + offset)


def generate_signature(secret: str, verb: str, url: str, expires: int, data: str) -> str:
    """Generates an API signature.

    A signature is HMAC_SHA256(secret, verb + path + nonce + data), hex encoded.
    Verb must be uppercased, url is relative, nonce must be an increasing 64-bit integer
    and the data, if present, must be JSON without whitespace between keys.

    For example, in psuedocode (and in real code below):

    verb=POST
    url=/api/v1/order
    expires=14169939957
    data={"symbol":"XBTZ14","quantity":1,"price":395.01}
    signature = HEX(HMAC_SHA256(secret,'POST/api/v1/order14169939957{"symbol":"XBTZ14","quantity":1,"price":395.01}'))

    """

    # Parse the url so we can remove the base and extract just the path.
    parsed_url = urlparse(url)
    path = parsed_url.path
    if parsed_url.query:
        path = path + '?' + parsed_url.query

    message = verb + path + str(expires) + data
    signature = hmac.new(bytes(secret, 'utf8'), bytes(message, 'utf8'), digestmod=hashlib.sha256).hexdigest()

    return signature


def generate_auth_headers(api_key: str, api_secret: str, verb: str, url: str, data: str) -> dict:
    """Generate ready-to-use headers to BitMEX API authentication. """

    headers = {}

    expires = generate_expires()
    signature = generate_signature(api_secret, verb, url, expires, data)

    headers['api-expires'] = str(expires)
    headers['api-key'] = api_key
    headers['api-signature'] = signature

    return headers

