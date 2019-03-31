import numpy as np
import pickle
from sklearn import preprocessing


def load_object(file_name):
    "load the pickled object"
    with open(file_name, 'rb') as f:
        return pickle.load(f)


def view_data(data_path):
    data = load_object(data_path)
    prices = data['prices']
    names = data['features']['names']
    features = data['features']['values']
    print(prices.shape)
    print(names)
    print(features.shape)
    return prices, features


class Strategy():
    def __init__(self):
        self.prev_price = np.empty()
        self.prev_factors = np.empty()
        self.prev_log_return = np.empty()

    def load_prev_return(self, price):
        daily_rf_rate = np.log(1.025) / 252 # daily risk-free rate
        self.prev_log_return = np.log(price) - np.log(self.prev_price) - daily_rf_rate

    def load_prev_data(self, price, factors):
        self.prev_factors = factors
        self.prev_price = price

    def handle_update(self, inx, price, factors):
        """Put your logic here
        Args:
            inx: zero-based inx in days
            price: [num_assets, ] 1*680
            factors: [num_assets, num_factors] 680*10
        Return:
            allocation: [num_assets, ] 1*680
        """
        # calculate the log return of last day
        self.load_prev_return(price)


        # load today's data as prev
        self.load_prev_data(price, factors)
        assert price.shape[0] == factors.shape[0]
        return np.array(np.random.rand() * price.shape[0])
