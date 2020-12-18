import datetime
import json
import time

import aiohttp

from aiobitmex import constants


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

    async def _make_request(
            self,
            path: str,
            query: str = None,
            postdict: dict = None,
            timeout: int = None,
            verb: str = None,
            max_retries=None
    ) -> dict:

        url = self.base_url + path

        if timeout is None:
            timeout = self.timeout

        # Default to POST if data is attached, GET otherwise
        if not verb:
            verb = 'POST' if postdict else 'GET'

        # By default don't retry POST or PUT. Retrying GET/DELETE is okay because they are idempotent.
        # In the future we could allow retrying PUT, so long as 'leavesQty' is not used (not idempotent),
        # or you could change the clOrdID (set {"clOrdID": "new", "origClOrdID": "old"}) so that an amend
        # can't erroneously be applied twice.
        # if max_retries is None:
        #     max_retries = 0 if verb in ['POST', 'PUT'] else 3

        # Auth
        # auth = APIKeyAuthWithExpires(self.apiKey, self.apiSecret)

        # async def retry() -> dict:
        #     self.retries += 1
        #     if self.retries > max_retries:
        #         raise Exception('Max retries on {} hit, raising.'.format(path, json.dumps(postdict or '')))
        #     return await self._make_request(path, query, postdict, timeout, verb, max_retries)

        # Make the request
        # try:
            # logger.info("sending req to %s: %s" % (url, json.dumps(postdict or query or '')))
        headers = {}

        response = await self.session.request(
            method=verb,
            url=url,
            params=query,
            headers=headers,
            json=postdict,
            timeout=timeout
        )
        # Make non-200s throw
        response.raise_for_status()

        # except requests.exceptions.HTTPError as e:
        #     if response is None:
        #         raise e
        #
        #     # 401 - Auth error. This is fatal.
        #     if response.status == 401:
        #         # logger.error("API Key or Secret incorrect, please check and restart.")
        #         # logger.error("Error: " + response.text)
        #         # if postdict:
        #             # logger.error(postdict)
        #         # Always exit, even if rethrow_errors, because this is fatal
        #         # exit(1)
        #
        #     # 404, can be thrown if order canceled or does not exist.
        #     elif response.status == 404:
        #         if verb == 'DELETE':
        #             # logger.error("Order not found: %s" % postdict['orderID'])
        #             return
        #         # logger.error("Unable to contact the BitMEX API (404). " +
        #         #              "Request: %s \n %s" % (url, json.dumps(postdict)))
        #         # exit_or_throw(e)
        #
        #     # 429, ratelimit; cancel orders & wait until X-RateLimit-Reset
        #     elif response.status == 429:
        #         # logger.error("Ratelimited on current request. Sleeping, then trying again. Try fewer " +
        #         #              "order pairs or contact support@bitmex.com to raise your limits. " +
        #         #              "Request: %s \n %s" % (url, json.dumps(postdict)))
        #
        #         # Figure out how long we need to wait.
        #         ratelimit_reset = response.headers['X-RateLimit-Reset']
        #         to_sleep = int(ratelimit_reset) - int(time.time())
        #         reset_str = datetime.datetime.fromtimestamp(int(ratelimit_reset)).strftime('%X')
        #
        #         # We're ratelimited, and we may be waiting for a long time. Cancel orders.
        #         # logger.warning("Canceling all known orders in the meantime.")
        #         # self.cancel([o['orderID'] for o in self.open_orders()])
        #
        #         # logger.error("Your ratelimit will reset at %s. Sleeping for %d seconds." % (reset_str, to_sleep))
        #         time.sleep(to_sleep)
        #
        #         # Retry the request.
        #         return retry()
        #
        #     # 503 - BitMEX temporary downtime, likely due to a deploy. Try again
        #     elif response.status == 503:
        #         # logger.warning("Unable to contact the BitMEX API (503), retrying. " +
        #         #                "Request: %s \n %s" % (url, json.dumps(postdict)))
        #         time.sleep(3)
        #         return retry()
        #
        #     elif response.status == 400:
        #         error = response.json()['error']
        #         message = error['message'].lower() if error else ''
        #
        #         # Duplicate clOrdID: that's fine, probably a deploy, go get the order(s) and return it
        #         if 'duplicate clordid' in message:
        #             orders = postdict['orders'] if 'orders' in postdict else postdict
        #
        #             IDs = json.dumps({'clOrdID': [order['clOrdID'] for order in orders]})
        #             orderResults = await self._make_request('/order', query={'filter': IDs}, verb='GET')
        #
        #             for i, order in enumerate(orderResults):
        #                 if (
        #                         order['orderQty'] != abs(postdict['orderQty']) or
        #                         order['side'] != ('Buy' if postdict['orderQty'] > 0 else 'Sell') or
        #                         order['price'] != postdict['price'] or
        #                         order['symbol'] != postdict['symbol']):
        #                     raise Exception(
        #                         'Attempted to recover from duplicate clOrdID, but order returned from API ' +
        #                         'did not match POST.\nPOST data: %s\nReturned order: %s' % (
        #                             json.dumps(orders[i]), json.dumps(order)))
        #             # All good
        #             return orderResults
        #
        #         elif 'insufficient available balance' in message:
        #             # logger.error('Account out of funds. The message: %s' % error['message'])
        #             # exit_or_throw(Exception('Insufficient Funds'))
        #             pass
        #
        #     # If we haven't returned or re-raised yet, we get here.
        #     # logger.error("Unhandled Error: %s: %s" % (e, response.text))
        #     # logger.error("Endpoint was: %s %s: %s" % (verb, path, json.dumps(postdict)))
        #     # exit_or_throw(e)
        #
        # except requests.exceptions.Timeout as e:
        #     # Timeout, re-run this request
        #     # logger.warning("Timed out on request: %s (%s), retrying..." % (path, json.dumps(postdict or '')))
        #     return await retry()
        #
        # except requests.exceptions.ConnectionError as e:
        #     # logger.warning("Unable to contact the BitMEX API (%s). Please check the URL. Retrying. " +
        #     #                "Request: %s %s \n %s" % (e, url, json.dumps(postdict)))
        #     time.sleep(1)
        #     return await retry()
        #
        # # Reset retry counter on success
        # self.retries = 0

        return await response.json()
