import asyncio
import json

import aiohttp

from aiobitmex import constants
from aiobitmex.auth import generate_auth_headers


class BitmexHTTP:
    """Async BitMEX API Connector."""

    def __init__(
            self, base_url: str = None,
            symbol: str = None,
            api_key: str = None,
            api_secret: str = None,
            prefix='aiobitmex',
            timeout=5
    ) -> None:

        self.base_url = base_url
        self.symbol = symbol

        if api_key is None or api_secret is None:
            raise Exception('Please set an API key and Secret to get started.')

        self.api_key = api_key
        self.api_secret = api_secret

        if len(prefix) > 13:
            raise ValueError('Order id prefix must be at most 13 characters long!')
        self.order_id_prefix = prefix

        self.retries = 0  # initialize counter
        self.timeout = timeout

        # Prepare HTTPS session
        self.session = aiohttp.ClientSession()
        # These headers are always sent
        self.session.headers.update({'user-agent': 'aiobitmex-' + constants.VERSION})
        self.session.headers.update({'content-type': 'application/json'})
        self.session.headers.update({'accept': 'application/json'})

    async def exit(self):
        await self.session.close()

    async def _make_request(
            self,
            path: str,
            verb: str,
            query: str = None,
            json_body: dict = None,
            timeout: int = None,
            max_retries=None
    ) -> dict:

        response = None

        url = self.base_url + path

        if timeout is None:
            timeout = self.timeout

        if max_retries is None:
            max_retries = 0

        async def retry() -> dict:
            self.retries += 1
            if self.retries > max_retries:
                raise Exception('Max retries on {} hit, raising.'.format(path, data))
            return await self._make_request(path, query, json_body, timeout, verb, max_retries)

        try:
            # Auth
            data = json.dumps(json_body) if json_body is not None else ''

            headers = generate_auth_headers(self.api_key, self.api_secret, verb, url, data)

            # Make the request
            response = await self.session.request(
                method=verb,
                url=url,
                params=query,
                headers=headers,
                json=json_body,
                timeout=timeout
            )
            # Throw non-200 errors
            response.raise_for_status()

        except aiohttp.ClientResponseError as e:
            message = e.message.lower()

            if response.status == 400:
                if 'insufficient available balance' in message:
                    # TODO: log message and raise appropriate exception
                    await self.exit()
                raise

        except aiohttp.ServerTimeoutError:
            # Timeout, re-run this request
            return await retry()

        except aiohttp.ClientConnectionError:
            await asyncio.sleep(1)
            return await retry()

        self.retries = 0

        return await response.json()
