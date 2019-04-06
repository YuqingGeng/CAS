import argparse
import random
import math

from client.exchange_service.client import BaseExchangeServerClient
from protos.order_book_pb2 import Order
from protos.service_pb2 import PlaceOrderResponse

# dictionary to track mid-market-price for each asset
asset_mid_prices = {}
# dictionaries for tracking current bid-asks; use for spread calculation
bids = {}
asks = {}

class ExampleMarketMaker(BaseExchangeServerClient):
    """A simple market making bot - shows the basics of subscribing
    to market updates and sending orders"""

    def __init__(self, *args, **kwargs):
        BaseExchangeServerClient.__init__(self, *args, **kwargs)

        self._orderids = set([])

    def _make_order(self, asset_code, quantity, base_price, spread, bid=True):
        return Order(asset_code = asset_code, quantity=quantity if bid else -1*quantity,
                     order_type = Order.ORDER_LMT,
                     price = base_price-spread/2 if bid else base_price+spread/2,
                     competitor_identifier = self._comp_id)

    def handle_exchange_update(self, exchange_update_response):
        print(exchange_update_response.competitor_metadata) # to monitor pnl
        '''
        print(exchange_update_response.competitor_metadata)
        # 10% of the time, cancel two arbitrary orders
        if random.random() < 0.10 and len(self._orderids) > 0:
            self.cancel_order(self._orderids.pop())
            self.cancel_order(self._orderids.pop())

        # only trade 5% of the time
        if random.random() > 0.05:
            return
        '''
        # store all updates to update aforementioned dictionary
        updates = exchange_update_response.market_updates
        for update in updates:
            # update the mid price for each asset
            mid_price = update.mid_market_price
            recent_asset_code = update.asset.asset_code
            asset_mid_prices[recent_asset_code] = mid_price
            # print(asset_mid_prices)

        """Blunt calendar spread strategy: shorter expiries should have lower
        yields, which means higher prices; if this pattern reverses we long
        the nearer month and short the future month

        Currently just trading the K and M futures
        """
        # thresholds based on empirical analysis aka eyeballing
        if asset_mid_prices["K"] - asset_mid_prices["M"] < -0.5:
            quantity = random.randrange(1, 10)
            spread = random.randrange(5, 10) # keeping this random spread
            round(2.665, 2)
            # long near
            bid_resp = self.place_order(self._make_order("K", quantity,
                round(asset_mid_prices["K"], 2), spread, True))
            # short far
            ask_resp = self.place_order(self._make_order("M", quantity,
                round(asset_mid_prices["M"], 2), spread, False))

            # check if order went through
            if type(bid_resp) != PlaceOrderResponse:
                print(bid_resp)
            else:
                self._orderids.add(bid_resp.order_id)
            if type(ask_resp) != PlaceOrderResponse:
                print(ask_resp)
            else:
                self._orderids.add(ask_resp.order_id)
        if asset_mid_prices["K"] - asset_mid_prices["M"] > 0.5:
            # short near long far:
            quantity = random.randrange(1, 10)
            """Next thing to work on: using the actual spread, dammit"""
            spread = random.randrange(5, 10)
            # long far
            bid_resp = self.place_order(self._make_order("M", quantity,
                round(asset_mid_prices["M"], 2), spread, True)) # round due to tick size requirement
            # short near
            ask_resp = self.place_order(self._make_order("K", quantity,
                round(asset_mid_prices["K"], 2), spread, False))

            # check if order went through
            if type(bid_resp) != PlaceOrderResponse:
                print(bid_resp)
            else:
                self._orderids.add(bid_resp.order_id)
            if type(ask_resp) != PlaceOrderResponse:
                print(ask_resp)
            else:
                self._orderids.add(ask_resp.order_id)

        """Keeping the horrible market making for now - not sure how this plays into case 1"""
        '''
        # place a bid and an ask for each asset
        for i, asset_code in enumerate(["K", "M", "N", "Q", "U", "V"]):
            # print("Asset code: ", asset_code)
            quantity = random.randrange(1, 10)
            base_price = random.randrange((i + 1) * 100, (i+1) * 150)
            spread = random.randrange(5, 10)

            bid_resp = self.place_order(self._make_order(asset_code, quantity,
                base_price, spread, True))
            ask_resp = self.place_order(self._make_order(asset_code, quantity,
                base_price, spread, False))

            if type(bid_resp) != PlaceOrderResponse:
                print(bid_resp)
            else:
                self._orderids.add(bid_resp.order_id)

            if type(ask_resp) != PlaceOrderResponse:
                print(ask_resp)
            else:
                self._orderids.add(ask_resp.order_id)
            '''

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run the exchange client')
    parser.add_argument("--server_host", type=str, default="localhost")
    parser.add_argument("--server_port", type=str, default="50052")
    parser.add_argument("--client_id", type=str)
    parser.add_argument("--client_private_key", type=str)
    parser.add_argument("--websocket_port", type=int, default=5678)

    args = parser.parse_args()
    host, port, client_id, client_pk, websocket_port = (args.server_host, args.server_port,
                                        args.client_id, args.client_private_key,
                                        args.websocket_port)

    client = ExampleMarketMaker(host, port, client_id, client_pk, websocket_port)
    client.start_updates()
