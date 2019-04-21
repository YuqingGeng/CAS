##### IMPORT PACKAGE #####
import argparse
import random
import math
import sys
import numpy as np
import pandas as pd
import scipy as sp

from client.exchange_service.client import BaseExchangeServerClient
from protos.order_book_pb2 import Order
from protos.service_pb2 import PlaceOrderResponse

# import quantopian.experimental.optimize as opt
# from quantopian.algorithm import calculate_optimal_portfolio
from statsmodels.tsa.stattools import coint

class Case1PairTrade(BaseExchangeServerClient):
    def __init__(self, *args, **kwargs):
        # Init BaseExchangeServerClient
        BaseExchangeServerClient.__init__(self, *args, **kwargs)

        self._orderids = set([])

        # Our future contract codes and pairs
        self.contract_list = ['K', 'M', 'N', 'Q', 'U', 'V']

        # redesign pair
        self.contract_pairs = [('K', 'M'), ('K', 'N'), ('K', 'Q'), ('K', 'U'), ('K', 'V'), \
                                ('M', 'N'), ('M', 'Q'), ('M', 'U'), ('M', 'V'), \
                                ('N', 'Q'), ('N', 'U'), ('N', 'V'), \
                                ('Q', 'U'), ('Q', 'V'), \
                                ('U', 'V')]

        # History Data

        self.hist_mid_price = {contract_code: None for contract_code in self.contract_list}

        # Record weights
        self.current_weights = {contract_code: 0 for contract_code in self.contract_list}

        # Record contract status
        self.in_Long = {contract_pair: False for contract_pair in self.contract_pairs}
        self.in_Short = {contract_pair: False for contract_pair in self.contract_pairs}

        # Time Windows
        self.long_ma = 10
        self.short_ma = 5

        self.counter = 0


    def _make_order(self, asset_code, quantity, base_price, spread, bid):
        # print("ORDER")
        # print(asset_code, quantity, base_price, spread, bid)
        order = Order(asset_code = asset_code, quantity=quantity if bid else -1*quantity,
                     order_type = Order.ORDER_LMT,
                     price = round(base_price-spread/2 if bid else base_price+spread/2, 2),
                     competitor_identifier = self._comp_id)
        # print(order)
        return order

    def handle_exchange_update(self, exchange_update_response):
        '''
        Handle the update from the exchange
        '''
        # call the server update
        updates = exchange_update_response.market_updates

        # print("#####", self.counter, "#####")
        for update in updates:
            contract = update.asset.asset_code
            mid_price = update.mid_market_price
            highest_bid = update.bids[0].price
            lowest_ask = update.asks[0].price
            # print(highest_bid, lowest_ask)
            spread = (lowest_ask - highest_bid) + 0.2

            # market making
            bid_resp = self.place_order(self._make_order(contract_y, y_quantity, lowest_ask + 0.02, 0.0, True))
            ask_resp = self.place_order(self._make_order(contract_x, x_quantity, highest_bid - 0.02, 0.0, False))

            if type(bid_resp) != PlaceOrderResponse:
                print("BID FOR ", contract_y, " FAILED: ", bid_resp)
            else:
                self._orderids.add(bid_resp.order_id)
            if type(ask_resp) != PlaceOrderResponse:
                print("ASK FOR ", contract_x, " FAILED: ", ask_resp)
            else:
                self._orderids.add(ask_resp.order_id)

            # print(contract, " ", mid_price)
            # load data
            if self.hist_mid_price[contract] == None:
                mid_prices = []
                mid_prices.append(mid_price)
                self.hist_mid_price[contract] = mid_prices
            else:
                self.hist_mid_price[contract].append(mid_price)

        # print(self.hist_mid_price)

        # train algorithm
        if self.counter >= 15:
            ready_weights = self.rebalance_pairs(spread)

        self.counter += 1
        competitor_metadata = exchange_update_response.competitor_metadata
        print(competitor_metadata)
        fills = exchange_update_response.fills
        # print(fills)


    def rebalance_pairs(self, spread):
        # Loop through every pair
        for contract_y, contract_x in self.contract_pairs:
            Y = np.asarray(self.hist_mid_price[contract_y])
            X = np.asarray(self.hist_mid_price[contract_x])

            y_log = np.log(Y)
            x_log = np.log(X)

            pvalue = coint(y_log, x_log)[1]

            if pvalue > 0.05:
                self.current_weights[contract_y] = 0
                self.current_weights[contract_x] = 0
                '''
                print(
                    "({} {}) no longer cointegrated, no new positions.".format(
                        contract_y,
                        contract_x
                    )
                )
                '''
                continue

            regression = sp.stats.linregress(x_log[-self.long_ma:], y_log[-self.long_ma:])

            spreads = Y - (regression.slope * X)

            zscore = (
                np.mean(spreads[-self.short_ma:]) - np.mean(spreads)
            ) / np.std(spreads, ddof=1)

            # print("z-score", zscore)

            hedge_ratio = regression.slope
            # print("hedge_ratio", hedge_ratio)

            if self.in_Short[(contract_y, contract_x)] and zscore < 0.0:
                # Do nothing but clean our trade
                self.current_weights[contract_y] = 0
                self.current_weights[contract_x] = 0

                self.in_Long[(contract_y, contract_x)] = False
                self.in_Short[(contract_y, contract_x)] = False
                continue

            if self.in_Long[(contract_y, contract_x)] and zscore > 0.0:
                # Do nothing but clean our trade
                self.current_weights[contract_y] = 0
                self.current_weights[contract_x] = 0

                self.in_Long[(contract_y, contract_x)] = False
                self.in_Short[(contract_y, contract_x)] = False
                continue

            if zscore < -0.3 and (not self.in_Long[(contract_y, contract_x)]): # long y short x
                # Only trade if NOT already in a trade
                y_target_contracts = 1
                x_target_contracts = hedge_ratio

                self.in_Long[(contract_y, contract_x)] = True
                self.in_Short[(contract_y, contract_x)] = False

                (y_target_pct, x_target_pct) = self.computeHoldingsPct(
                    y_target_contracts,
                    x_target_contracts,
                    Y[-1],
                    X[-1]
                )

                y_quantity = int(25*math.ceil(y_target_pct))
                x_quantity = int(25*math.ceil(x_target_pct))

                y_price = round(Y[-1], 2)
                x_price = round(X[-1], 2)
                spread = round(spread, 2)

                #asset_code, quantity, base_price, spread, bid=True):
                bid_resp = self.place_order(self._make_order(contract_y, y_quantity, y_price, spread, True))
                ask_resp = self.place_order(self._make_order(contract_x, x_quantity, x_price, spread, False))

                self.current_weights[contract_y] = self.current_weights[contract_y]+ y_target_pct
                self.current_weights[contract_x] = self.current_weights[contract_x] - x_target_pct

                if type(bid_resp) != PlaceOrderResponse:
                    print("BID FOR ", contract_y, " FAILED: ", bid_resp)
                else:
                    self._orderids.add(bid_resp.order_id)
                if type(ask_resp) != PlaceOrderResponse:
                    print("ASK FOR ", contract_x, " FAILED: ", ask_resp)
                else:
                    self._orderids.add(ask_resp.order_id)
                continue

            if zscore > 0.3 and (not self.in_Short[(contract_y, contract_x)]):
                # Only trade if NOT already in a trade
                y_target_contracts = 1
                x_target_contracts = hedge_ratio

                self.in_Long[(contract_y, contract_x)] = False
                self.in_Short[(contract_y, contract_x)] = True

                (y_target_pct, x_target_pct) = self.computeHoldingsPct(
                    y_target_contracts,
                    x_target_contracts,
                    Y[-1],
                    X[-1]
                )

                y_quantity = int(25*math.ceil(y_target_pct))
                x_quantity = int(25*math.ceil(x_target_pct))
                y_price = round(Y[-1], 2)
                x_price = round(X[-1], 2)
                spread = round(spread, 2)

                ask_resp = self.place_order(self._make_order(contract_y, y_quantity, y_price, spread, False))
                bid_resp = self.place_order(self._make_order(contract_x, x_quantity, x_price, spread, True))
                '''
                self.current_weights[contract_y] = self.current_weights[contract_y] - y_target_pct
                self.current_weights[contract_x] = self.current_weights[contract_x] + x_target_pct
                '''
                if type(bid_resp) != PlaceOrderResponse:
                    print("BID FOR ", contract_x, " FAILED: ", bid_resp)
                else:
                    self._orderids.add(bid_resp.order_id)
                if type(ask_resp) != PlaceOrderResponse:
                    print("ASK FOR ", contract_y, " FAILED: ", ask_resp)
                else:
                    self._orderids.add(ask_resp.order_id)
                continue



        weights = self.current_weights
        return weights

    def computeHoldingsPct(self, yShares, xShares, yPrice, xPrice):
        yDol = yShares * yPrice
        xDol = xShares * xPrice
        notionalDol =  abs(yDol) + abs(xDol)
        y_target_pct = yDol / notionalDol
        x_target_pct = xDol / notionalDol
        return (y_target_pct, x_target_pct)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run the exchange client')
    parser.add_argument("--server_host", type=str, default="ec2-52-15-48-176.us-east-2.compute.amazonaws.com") # ec2-13-59-156-215.us-east-2.compute.amazonaws.com
    parser.add_argument("--server_port", type=str, default="50052")
    parser.add_argument("--client_id", type=str)
    parser.add_argument("--client_private_key", type=str)
    parser.add_argument("--websocket_port", type=int, default=5678)

    args = parser.parse_args()
    host, port, client_id, client_pk, websocket_port = (args.server_host, args.server_port,
                                        "SmithAmh", args.client_private_key,
                                        args.websocket_port)

    client = Case1PairTrade(host, port, client_id, client_pk, websocket_port)
    client.start_updates()
