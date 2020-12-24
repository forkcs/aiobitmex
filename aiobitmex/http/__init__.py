import asyncio
import json
import time
from typing import List, Union

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

    async def exit(self) -> None:
        await self.session.close()

    # START ENDPOINTS #

    ################
    # Announcement #
    ################

    async def get_announcement(self, columns: List[str] = None) -> Union[List[dict], dict]:
        params = None
        if columns is not None:
            params = {'columns': columns}
        return await self._make_request(path='/announcement', verb='GET', query=params)

    async def get_urgent_announcement(self) -> Union[List[dict], dict]:
        return await self._make_request(path='/announcement/urgent', verb='GET')

    ###########
    # API Key #
    ###########

    async def get_api_keys(self, reverse: bool = False) -> Union[List[dict], dict]:
        params = {'reverse': reverse}
        return await self._make_request(path='/apiKey', verb='GET', query=params)

    ########
    # Chat #
    ########

    async def get_chat(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def post_chat(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_chat_channels(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_chat_connected(self) -> Union[List[dict], dict]:
        raise NotImplemented

    #############
    # Execution #
    #############

    async def get_execution(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_trade_history(self) -> Union[List[dict], dict]:
        raise NotImplemented

    ###########
    # Funding #
    ###########

    async def get_funding(self) -> Union[List[dict], dict]:
        raise NotImplemented

    ########################
    # Global Notifications #
    ########################

    async def get_global_notification(self) -> Union[List[dict], dict]:
        raise NotImplemented

    ##############
    # Instrument #
    ##############

    async def get_instrument(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_active_instrument(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_active_and_indices(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_active_intervals(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_composite_index(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_indices(self) -> Union[List[dict], dict]:
        raise NotImplemented

    #############
    # Insurance #
    #############

    async def get_insurance(self) -> Union[List[dict], dict]:
        raise NotImplemented

    ###############
    # Leaderboard #
    ###############

    async def get_leaderboard(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_leaderboard_name(self) -> Union[List[dict], dict]:
        raise NotImplemented

    ###############
    # Liquidation #
    ###############

    async def get_liquidation(self) -> Union[List[dict], dict]:
        raise NotImplemented

    #########
    # Order #
    #########

    async def get_order(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def put_order(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def post_order(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def delete_order(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def delete_all_orders(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def put_bulk_orders(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def post_bulk_orders(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def cancel_all_after(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def close_position(self) -> Union[List[dict], dict]:
        raise NotImplemented

    #############
    # OrderBook #
    #############

    async def get_l2_orderbook(self) -> Union[List[dict], dict]:
        raise NotImplemented

    ############
    # Position #
    ############

    async def get_position(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def post_position_isolate(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def post_leverage(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def post_risklimit(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def transfer_margin(self) -> Union[List[dict], dict]:
        raise NotImplemented

    #########
    # Quote #
    #########

    async def get_quote(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_quote_bucketed(self) -> Union[List[dict], dict]:
        raise NotImplemented

    ##########
    # Schema #
    ##########

    async def get_schema(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_websocket_schema(self) -> Union[List[dict], dict]:
        raise NotImplemented

    ##############
    # Settlement #
    ##############

    async def get_settlement(self) -> Union[List[dict], dict]:
        raise NotImplemented

    #########
    # Stats #
    #########

    async def get_stats(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_stats_history(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_stats_history_usd(self) -> Union[List[dict], dict]:
        raise NotImplemented

    #########
    # Trade #
    #########

    async def get_trade(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_trade_bucketed(self) -> Union[List[dict], dict]:
        raise NotImplemented

    ########
    # User #
    ########

    async def get_user(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_affilate_status(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def cancel_withdrawal(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def check_referral_code(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_user_commission(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def post_communication_token(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def confirm_email(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def confirm_withdrawal(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_deposit_address(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_execution_history(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def logout(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_margin(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_min_withdrawal_fee(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def post_preferences(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_quote_fill_ratio(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_quote_value_ratio(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def request_withdrawal(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_wallet(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_wallet_history(self) -> Union[List[dict], dict]:
        raise NotImplemented

    async def get_wallet_summary(self) -> Union[List[dict], dict]:
        raise NotImplemented

    #############
    # UserEvent #
    #############

    async def get_user_event(self) -> Union[List[dict], dict]:
        raise NotImplemented

    # END ENDPOINTS #

    async def _make_request(
            self,
            path: str,
            verb: str,
            query: str = None,
            json_body: dict = None,
            timeout: int = None,
            max_retries=None
    ) -> Union[List[dict], dict]:

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

            # 401, unauthorized; this is fatal, always exit
            elif response.status == 401:
                # TODO: log message and raise appropriate exception
                await self.exit()

            # 429, ratelimit; cancel orders and wait until X-RateLimit-Reset
            elif response.status == 429:
                # Figure out how long we need to wait
                ratelimit_reset = response.headers['X-RateLimit-Reset']

                to_sleep = int(ratelimit_reset) - int(time.time())
                # TODO: We're ratelimited, and we may be waiting for a long time. Cancel orders.

                await asyncio.sleep(to_sleep)

                # Retry the request
                return await retry()

            # BitMEX is downtime now, just wait and retry
            elif response.status == 503:
                await asyncio.sleep(2.5)
                return await retry()

        except aiohttp.ServerTimeoutError:
            # Timeout, re-run this request
            return await retry()

        except aiohttp.ClientConnectionError:
            await asyncio.sleep(1)
            return await retry()

        self.retries = 0

        return await response.json()
