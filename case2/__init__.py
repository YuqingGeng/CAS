import argparse
import random
import py_vollib.black_scholes.greeks.analytical as bsga
import numpy as np
import time
import math
import py_vollib.black_scholes as bs
from py_vollib.black.implied_volatility import implied_volatility as IV

from client.exchange_service.client import BaseExchangeServerClient
from protos.order_book_pb2 import Order
from protos.service_pb2 import PlaceOrderResponse

# underlying price book
# buy call sell call in a range(28, 32)
# calculate delta vega for the PORTFOLIO

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

        self.deltas = {'C98PHX': 0,
                        'P98PHX': 0,
                        'C99PHX': 0,
                        'P99PHX': 0,
                        'C100PHX': 0,
                        'P100PHX': 0,
                        'C101PHX': 0,
                        'P101PHX': 0,
                        'C102PHX': 0,
                        'P102PHX': 0,
                        'IDX#PHX': 1}

        self.vegas = {'C98PHX': 0,
                        'P98PHX': 0,
                        'C99PHX': 0,
                        'P99PHX': 0,
                        'C100PHX': 0,
                        'P100PHX': 0,
                        'C101PHX': 0,
                        'P101PHX': 0,
                        'C102PHX': 0,
                        'P102PHX': 0}

        self.delta_ptflo = 0
        self.vega_ptflo = 0
        self.start_time = time.time()
        self.net_exposure = 0

        self.bid_dict = {}
        self.ask_dict = {}
        self.mid_dict = {}


    def _make_order(self, asset_code, quantity, base_price, spread, bid=True):
        return Order(asset_code = asset_code, quantity=quantity if bid else -1*quantity,
                     order_type = Order.ORDER_LMT,
                     price = round(base_price-spread/2 if bid else base_price+spread/2, 2),
                     competitor_identifier = self._comp_id)


    # update delta
    def update_delta(self, asset_code):
        flag = asset_code[0].lower()
        S = self.mid_dict['IDX#PHX']
        if len(asset_code) == 6:
            K = int(asset_code[1:3])
        elif len(asset_code) == 7:
            K = int(asset_code[1:4])
        t = (63-(time.time()-self.start_time)/(900/21))/252 #time to expiration in years
        r = 0
        price = self.mid_dict[asset_code]
        sigma = IV(price, S, K, r, t, flag)* np.sqrt(252)
        return bsga.delta(flag, S, K, t, r, sigma)

    # update vega
    def update_vega(self, asset_code):
        flag = asset_code[0].lower()
        S = self.mid_dict['IDX#PHX']
        if len(asset_code) == 6:
            K = int(asset_code[1:3])
        elif len(asset_code) == 7:
            K = int(asset_code[1:4])
        t = (63-(time.time()-self.start_time)/(900/21))/252 #time to expiration in years
        r = 0
        price = self.mid_dict[asset_code]
        sigma = IV(price, S, K, r, t, flag)* np.sqrt(252)
        return bsga.vega(flag, S, K, t, r, sigma)

    def make_market(self, asset_code):
        best_bid = max(self.bid_dict[asset_code])
        best_ask = min(self.ask_dict[asset_code])
        #print(best_bid, best_ask)

        quantity = int(round(random.randrange(1, 5)))
        #base_price = self.mid_dict[asset_code]
        spread = best_ask-best_bid

        bid_resp = self.place_order(self._make_order(asset_code, quantity,
            best_bid, spread, True))
        ask_resp = self.place_order(self._make_order(asset_code, quantity,
            best_ask, spread, False))

        if type(bid_resp) != PlaceOrderResponse:
            print(bid_resp)
        else:
            self._orderids.add(bid_resp.order_id)

        if type(ask_resp) != PlaceOrderResponse:
            print(ask_resp)
        else:
            self._orderids.add(ask_resp.order_id)

    #def delta_ptflo(asset_code)



    def handle_exchange_update(self, exchange_update_response):
        self.delta_ptflo = sum([x*y for x,y in zip(self.deltas.values(),self.positions.values())])
        #print(self.positions)
        #print(abs(self.delta_ptflo))
        self.vega_ptflo=sum([x*y for x,y in zip(self.positions.values(),self.vegas.values())])
        print(abs(self.delta_ptflo), abs(self.vega_ptflo))
        market_updates = exchange_update_response.market_updates


        for update in market_updates:
            bid_list = []
            ask_list = []
            bid_quant = []
            ask_quant = []


            asset_id = update.asset
            #print(asset_id)
            asset_code = asset_id.asset_code

            bids = update.bids
            asks = update.asks
            mid_price = update.mid_market_price

            self.mid_dict[asset_code] = mid_price
            #print(asset_code, mid_price)

            for bid in bids:
                bid_list.append(bid.price)
                bid_quant.append(bid.size)
                self.bid_dict[asset_code] = bid_list

            for ask in asks:
                ask_list.append(ask.price)
                ask_quant.append(ask.size)
                self.ask_dict[asset_code] = ask_list

            if asset_code == 'IDX#PHX':
                lo = math.floor(self.mid_dict['IDX#PHX'])
                hi = math.ceil(self.mid_dict['IDX#PHX'])
                if lo in [98, 99, 100, 101] and hi in [99, 100, 101, 102]:
                    best_bid = self.mid_dict['C'+str(lo) + 'PHX']
                    best_ask = self.mid_dict['C'+str(hi) + 'PHX']

                    spread = best_ask-best_bid
                    #print(lo, self.mid_dict['IDX#PHX'], hi)
                    quantity = 5
                    bid_resp = self.place_order(self._make_order('C'+str(lo)+'PHX', quantity,
                        best_bid, spread, True))
                    ask_resp = self.place_order(self._make_order('C'+str(hi)+'PHX', quantity,
                        best_ask, spread, False))

                    if type(bid_resp) != PlaceOrderResponse:
                        print(bid_resp)
                    else:
                        self._orderids.add(bid_resp.order_id)

                    if type(ask_resp) != PlaceOrderResponse:
                        print(ask_resp)
                    else:
                        self._orderids.add(ask_resp.order_id)

                    # best_bid2 = self.mid_dict['P'+str(lo) + 'PHX']
                    # best_ask2 = self.mid_dict['P'+str(hi) + 'PHX']
                    # spread2 = best_ask2-best_bid2
                    # if spread > 0:
                    #     bid_resp = self.place_order(self._make_order('P'+str(hi)+'PHX', 1,
                    #         best_ask2, spread, True))
                    #     ask_resp = self.place_order(self._make_order('P'+str(lo)+'PHX', 1,
                    #         best_bid2, spread, False))
                    #
                    #     if type(bid_resp) != PlaceOrderResponse:
                    #         print(bid_resp)
                    #     else:
                    #         self._orderids.add(bid_resp.order_id)
                    #
                    #     if type(ask_resp) != PlaceOrderResponse:
                    #         print(ask_resp)
                    #     else:
                    #         self._orderids.add(ask_resp.order_id)


            else:
                self.make_market(asset_code)


        competitor_metadata = exchange_update_response.competitor_metadata
        ##### print pnl ######
        print(competitor_metadata)
        fills = exchange_update_response.fills

        # update positions
        for fill in fills:
            #print(fill)
            filled_quantity = fill.filled_quantity
            order = fill.order
            #print(order)
            asset_code = order.asset_code
            quantity = order.quantity
            order_id = order.order_id
            if quantity > 0:
                self.positions[asset_code] = filled_quantity
                if quantity != filled_quantity:
                    self.cancel_order(order_id)
            elif quantity < 0:
                self.positions[asset_code] = -filled_quantity
                if -quantity != filled_quantity:
                    self.cancel_order(order_id)






        # 10% of the time, cancel two arbitrary orders
        #if random.random() < 0.10 and len(self._orderids) > 0:
            #self.cancel_order(self._orderids.pop())
            #self.cancel_order(self._orderids.pop())

        # only trade 5% of the time
        #if random.random() > 0.05:
            #return

        # market making


        # manage inventory
        for asset_code in self.deltas.keys():
            #print(asset_code)
            if asset_code != 'IDX#PHX':
                self.deltas[asset_code] = self.update_delta(asset_code)
                self.vegas[asset_code] = self.update_vega(asset_code)

        if self.delta_ptflo > 0:
            quantity = round(self.delta_ptflo)
            price = min(self.bid_dict['IDX#PHX'])
            spread = np.mean(self.ask_dict['IDX#PHX']) - np.mean(self.bid_dict['IDX#PHX'])
            sell_under = self.place_order(self._make_order('IDX#PHX', quantity,
                price, spread, False))

            if type(sell_under) != PlaceOrderResponse:
                print(sell_under)
            else:
                self._orderids.add(sell_under.order_id)

        elif self.delta_ptflo < 0:
            quantity = abs(round(self.delta_ptflo))
            price = max(self.ask_dict['IDX#PHX'])
            spread = np.mean(self.ask_dict['IDX#PHX']) - np.mean(self.bid_dict['IDX#PHX'])
            buy_under = self.place_order(self._make_order('IDX#PHX', quantity,
                price, spread, True))

            if type(buy_under) != PlaceOrderResponse:
                print(buy_under)
            else:
                self._orderids.add(buy_under.order_id)








                # self.delta_ptflo = abs(sum([x*y for x,y in zip(self.positions.values(),self.deltas.values())]))
                # price = min(self.ask_dict[asset_code])
                # spread = np.mean(self.ask_dict[asset_code]) - np.mean(self.bid_dict[asset_code])
                # quantity = 40
                #
                # if self.delta_ptflo>10:
                #     if asset_code[0] == 'C' and self.deltas[asset_code]>0:
                #         sell_under = self.place_order(self._make_order(asset_code, quantity,
                #             price, spread, False))
                #         if type(sell_under) != PlaceOrderResponse:
                #             print(sell_under)
                #         else:
                #             self._orderids.add(sell_under.order_id)
                #     elif asset_code[0] == 'C' and self.deltas[asset_code]<0:
                #         buy_under= self.place_order(self._make_order(asset_code, quantity,
                #             price, spread, True))
                #         if type(buy_under) != PlaceOrderResponse:
                #             print(buy_under)
                #         else:
                #             self._orderids.add(buy.order_id)
                #     elif asset_code[0] == 'P' and self.deltas[asset_code]>0:
                #         buy_under= self.place_order(self._make_order(asset_code, quantity,
                #           price, spread, True))
                #         if type(buy_under) != PlaceOrderResponse:
                #             print(buy_under)
                #         else:
                #             self._orderids.add(buy.order_id)
                #     elif asset_code[0] == 'P' and self.deltas[asset_code]<0:
                #          sell_under= self.place_order(self._make_order(asset_code, quantity,
                #         price, spread, False))
                #         if type(sell_under) != PlaceOrderResponse:
                #             print(sell_under)
                #         else:
                #             self._orderids.add(sell_under.order_id)
                # elif self.delta_ptflo<-10:
                #     if asset_code[0] == 'C' and self.deltas[asset_code]>0:
                #         buy_under = self.place_order(self._make_order(asset_code, quantity,
                #             price, spread, True))
                #         if type(buy_under) != PlaceOrderResponse:
                #             print(buy_under)
                #         else:
                #             self._orderids.add(buy.order_id)
                #     elif asset_code[0] == 'C' and self.deltas[asset_code]<0:
                #         sell_under= self.place_order(self._make_order(asset_code, quantity,
                #             price, spread, False))
                #         if type(sell_under) != PlaceOrderResponse:
                #             print(sell_under)
                #         else:
                #             self._orderids.add(sell_under.order_id)
                #     elif asset_code[0] == 'P' and self.deltas[asset_code]>0:
                #         sell_under= self.place_order(self._make_order(asset_code, quantity,
                #           price, spread, False))
                #         if type(sell_under) != PlaceOrderResponse:
                #             print(sell_under)
                #         else:
                #             self._orderids.add(sell_under.order_id)
                #     elif asset_code[0] == 'P' and self.deltas[asset_code]<0:
                #         buy_under= self.place_order(self._make_order(asset_code, quantity,
                #         price, spread, True))
                #         if type(buy_under) != PlaceOrderResponse:
                #             print(buy_under)
                #         else:
                #             self._orderids.add(buy.order_id)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run the exchange client')
    parser.add_argument("--server_host", type=str, default="ec2-52-15-48-176.us-east-2.compute.amazonaws.com")
    parser.add_argument("--server_port", type=str, default="50052")
    parser.add_argument("--client_id", type=str)
    parser.add_argument("--client_private_key", type=str)
    parser.add_argument("--websocket_port", type=int, default=5678)

    args = parser.parse_args()
    host, port, client_id, client_pk, websocket_port = (args.server_host, args.server_port,
                                        "SmithAmh", args.client_private_key,
                                        args.websocket_port)

    client = ExampleMarketMaker(host, port, client_id, client_pk, websocket_port)
    client.start_updates()
