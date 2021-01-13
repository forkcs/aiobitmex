import asyncio
import datetime
import json
import time
from decimal import Decimal
from typing import List, Union, Optional

import aiohttp

from aiobitmex import constants
from aiobitmex.auth import generate_auth_headers


class BitmexHTTP:
    """Async BitMEX API Connector."""

    def __init__(
            self,
            base_url: Optional[str] = None,
            symbol: Optional[str] = None,
            api_key: Optional[str] = None,
            api_secret: Optional[str] = None,
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

    async def get_executions(
            self,
            symbol: Optional[str] = None,
            _filter: Optional[dict] = None,
            columns: Optional[List[str]] = None,
            count: int = 100,
            start: Optional[int] = None,
            reverse: bool = False,
            start_time: Optional[datetime.datetime] = None,
            end_time: Optional[datetime.datetime] = None
    ) -> Union[List[dict], dict]:
        """Implements GET /execution."""

        params = {}

        if symbol is None:
            symbol = self.symbol

        params['symbol'] = symbol
        params['count'] = count
        params['reverse'] = reverse

        if _filter is not None:
            params['filter'] = _filter
        if columns is not None:
            params['columns'] = columns
        if start is not None:
            params['start'] = start
        if start_time is not None:
            params['startTime'] = start_time
        if end_time is not None:
            params['endTime'] = end_time

        return await self._make_request(path='/execution', verb='GET', query=params)

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

    async def get_orders(
            self,
            symbol: Optional[str] = None,
            _filter: Optional[dict] = None,
            columns: Optional[List[str]] = None,
            count: int = 100,
            start: Optional[int] = None,
            reverse: bool = False,
            start_time: Optional[datetime.datetime] = None,
            end_time: Optional[datetime.datetime] = None
    ) -> List[dict]:
        """Implements GET /order."""

        params = {}

        if symbol is None:
            symbol = self.symbol

        params['symbol'] = symbol
        params['count'] = count
        params['reverse'] = reverse

        if _filter is not None:
            params['filter'] = _filter
        if columns is not None:
            params['columns'] = columns
        if start is not None:
            params['start'] = start
        if start_time is not None:
            params['startTime'] = start_time
        if end_time is not None:
            params['endTime'] = end_time

        return await self._make_request(path='/order', verb='GET', query=params)

    async def amend_order(
            self,
            order_id: Optional[str] = None,
            clordid: Optional[str] = None,
            origclordid: Optional[str] = None,
            order_qty: Optional[int] = None,
            leaves_qty: Optional[int] = None,
            price: Optional[Decimal] = None,
            stop_px: Optional[Decimal] = None,
            peg_offset_value: Optional[Decimal] = None,
            text: Optional[str] = None
    ) -> dict:
        """Implements PUT /order."""

        body = {}

        if order_id is not None:
            body['orderID'] = order_id
        if clordid is not None:
            body['clOrdID'] = clordid
        if origclordid is not None:
            body['origClOrdID'] = origclordid
        if order_qty is not None:
            body['orderQty'] = order_qty
        if leaves_qty is not None:
            body['leavesQty'] = leaves_qty
        if price is not None:
            body['price'] = price
        if stop_px is not None:
            body['stopPx'] = stop_px
        if peg_offset_value is not None:
            body['pegOffsetValue'] = peg_offset_value
        if text is not None:
            body['text'] = text

        return await self._make_request('/order', 'PUT', json_body=body, max_retries=0)

    async def post_order(
            self,
            symbol: Optional[str] = None,
            side: Optional[str] = None,
            order_qty: Optional[int] = None,
            price: Optional[Decimal] = None,
            display_qty: Optional[int] = None,
            stop_px: Optional[Decimal] = None,
            clordid: Optional[str] = None,
            peg_offset_value: Optional[Decimal] = None,
            peg_price_type: Optional[str] = None,
            ord_type: Optional[str] = None,
            time_in_force: Optional[str] = None,
            exec_inst: Optional[str] = None,
            text: Optional[str] = None
    ) -> dict:
        """Implements POST /order."""

        body = {}

        if symbol is None:
            symbol = self.symbol

        body['symbol'] = symbol

        if side is not None:
            body['side'] = side
        if order_qty is not None:
            body['orderQty'] = order_qty
        if price is not None:
            body['price'] = price
        if display_qty is not None:
            body['displayQty'] = display_qty
        if stop_px is not None:
            body['stopPx'] = stop_px
        if clordid is not None:
            body['clOrdID'] = clordid
        if peg_offset_value is not None:
            body['pegOffsetValue'] = peg_offset_value
        if peg_price_type is not None:
            body['pegPriceType'] = peg_price_type
        if ord_type is not None:
            body['ordType'] = ord_type
        if time_in_force is not None:
            body['timeInForce'] = time_in_force
        if exec_inst is not None:
            body['execInst'] = exec_inst
        if text is not None:
            body['text'] = text

        return await self._make_request('/order', 'POST', json_body=body, max_retries=0)

    async def cancel_order(
            self,
            order_id: Optional[str] = None,
            clordid: Optional[str] = None,
            text: Optional[str] = None
    ) -> List[dict]:
        """Implements DELETE /order."""

        body = {}

        if order_id is not None:
            body['orderID'] = order_id
        if clordid is not None:
            body['clOrdID'] = clordid
        if text is not None:
            body['text'] = text

        return await self._make_request('/order', 'DELETE', max_retries=3)

    async def cancel_all_orders(self) -> List[dict]:
        """Implements DELETE /order/all."""

        return await self._make_request('/order/all', 'DELETE', max_retries=3)

    async def bulk_amend_orders(self, orders: List) -> List[dict]:
        """Implements PUT /order/bulk."""

        body = {'orders': orders}

        return await self._make_request('/order/bulk', 'PUT', json_body=body, max_retries=1)

    async def bulk_post_orders(self, orders: List) -> List[dict]:
        """Implements POST /order/bulk."""

        body = {'orders': orders}

        return await self._make_request('/order/bulk', 'POST', json_body=body, max_retries=1)

    async def cancel_all_after(self, timeout: int) -> dict:
        """Implements POST /order/cancelAllAfter."""

        body = {'timeout': timeout}

        return await self._make_request('/order/cancelAllAfter', 'POST', json_body=body, max_retries=1)

    async def close_position(
            self,
            symbol: str = None,
            price: Decimal = None
    ) -> dict:
        """Implements POST /order/closePosition."""

        body = {}

        if symbol is None:
            symbol = self.symbol

        body['symbol'] = symbol

        if price is not None:
            body['price'] = price

        return await self._make_request('/order/closePosition', 'POST', json_body=body, max_retries=3)

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

        # TODO: join url parts more safely and properly
        url = self.base_url + path

        if timeout is None:
            timeout = self.timeout

        if max_retries is None:
            max_retries = 0

        async def retry() -> Union[List[dict], dict]:
            self.retries += 1
            if self.retries > max_retries:
                raise Exception('Max retries on {} hit, raising.'.format(path, data))
            return await self._make_request(path, query, json_body, timeout, verb, max_retries)

        # Auth
        data = json.dumps(json_body) if json_body is not None else ''
        headers = generate_auth_headers(self.api_key, self.api_secret, verb, url, data)

        # Make the request
        async with self.session.request(
            method=verb,
            url=url,
            params=query,
            headers=headers,
            json=json_body,
            timeout=timeout
        ) as response:
            try:
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
                    raise

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
