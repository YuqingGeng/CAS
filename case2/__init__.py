import argparse
import random
import py_vollib.black_scholes.greeks.analytical as bsga


from client.exchange_service.client import BaseExchangeServerClient
from protos.order_book_pb2 import Order
from protos.service_pb2 import PlaceOrderResponse


class ExampleMarketMaker(BaseExchangeServerClient):
    """A simple market making bot - shows the basics of subscribing
    to market updates and sending orders"""

    def __init__(self, *args, **kwargs):
        BaseExchangeServerClient.__init__(self, *args, **kwargs)
        self._orderids = set([])
        self.positions = {'C98PHX': 0,
                          'P98PHX': 0,
                          'C99PHX': 0,
                          'P99PHX': 0,
                          'C100PHX': 0,
                          'P100PHX': 0,
                          'C101PHX': 0,
                          'P101PHX': 0,
                          'C102PHX': 0,
                          'P102PHX': 0,
                          'IDX#PHX': 0}
        self.delta = 0
        self.vega = 0

        self.delta_limit = 25
        self.vega_limit = 25

    def _make_order(self, asset_code, quantity, base_price, spread, bid=True):
        return Order(asset_code = asset_code, quantity=quantity if bid else -1*quantity,
                     order_type = Order.ORDER_LMT,
                     price = base_price-spread/2 if bid else base_price+spread/2,
                     competitor_identifier = self._comp_id)
    # update delta
    def update_delta(flag, S, K, t, r, sigma):
        self.delta = bsga.delta(flag, S, K, t, r, sigma)

    # update vega
    def update_vega(flag, S, K, t, r, sigma):
        self.vega = bsga.vega(flag, S, K, t, r, sigma)

    def handle_exchange_update(self, exchange_update_response):
        market_updates = exchange_update_response.market_updates

        for update in market_updates:
            mid_price = update.mid_market_price
            asset_id = update.asset
            bids = update.bids
            asks = update.asks
            for bid in bids:
                bid_price = bid.price
                #print(bid_price)
            #bid = update.bid
            #ask = update.ask
        #print(update.asset)#, asset_id, bids, asks)

        competitor_metadata = exchange_update_response.competitor_metadata
        print(competitor_metadata)
        fills = exchange_update_response.fills

        #update positionss
        for fill in fills:
            filled_quantity = fill.filled_quantity
            order = fill.order
            asset_code = order.asset_code
            quantity = order.quantity
            if quantity > 0:
                self.positions[asset_code] += filled_quantity
            elif quantity < 0:
                self.positions[asset_code] -= filled_quantity
            #print(self.positions)


        # 10% of the time, cancel two arbitrary orders
        if random.random() < 0.10 and len(self._orderids) > 0:
            self.cancel_order(self._orderids.pop())
            self.cancel_order(self._orderids.pop())

        # only trade 5% of the time
        if random.random() > 0.05:
            return

        # place a bid and an ask for each asset
        for i, asset_code in enumerate(self.positions.keys()):
            quantity = random.randrange(1, 100)
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
