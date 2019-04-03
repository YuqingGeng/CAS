# import packages
import numpy as np
import pandas as pd
import scipy.stats as stats
import pickle
from sklearn.preprocessing import RobustScaler
from sklearn.linear_model import LinearRegression
from sklearn.decomposition import PCA

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
        self.yesterday_price = 0
        self.prev_factors =  None # N*A*F
        self.prev_log_return = None #N*A
        self.count_day = 0

    def load_prev_return(self, price):
        daily_rf_rate = np.log(1.025) / 252 # daily risk-free rate
        prev_return = np.log(price) - np.log(self.yesterday_price) - daily_rf_rate

        if self.count_day == 1:
            self.prev_log_return = np.array([prev_return])
        else:
            self.prev_log_return = np.append(self.prev_log_return, [prev_return], axis=0)

    def load_prev_data(self, price, factors):
        if self.count_day == 0:
            self.prev_factors = np.array([factors])
        else:
            self.prev_factors = np.append(self.prev_factors, [factors], axis=0)

        self.yesterday_price = price

    def handle_update(self, inx, price, factors):
        """Put your logic here
        Args:
            inx: zero-based inx in days
            price: [num_assets, ] 1*680
            factors: [num_assets, num_factors] 680*10
        Return:
            allocation: [num_assets, ] 1*680
        """
        self.count_day = inx
        # calculate the log return of last day
        print("######", self.count_day, "######")
        if self.count_day != 0:
            self.load_prev_return(price)

        A, F = factors.shape

        if self.count_day >= 10:
            # standardize features
            scaler = RobustScaler()
            # copy
            f_series_std = self.prev_factors
            for f in range(F):
                feature = f_series_std[:, :, f]
                feature_std = scaler.fit_transform(feature)
                f_series_std[:, :, f] = feature_std

            # PCA matrix
            # loop through every asset
            pca_tranform = []
            pca_num = 5
            for a in range(A):
                x = f_series_std[:, a, :] #N*F
                y = self.prev_log_return[:, a] #N*1
                x = x.T
                pca = PCA(n_components = pca_num, svd_solver = 'auto') # auto = default; choose between 6 and 7
                principal_factor = pca.fit_transform(x)
                pca_tranform.append(principal_factor)

            # Linear Regression
            lm = LinearRegression()
            # X_af is the exposure of asset a to factor f
            X_af = np.empty([A,pca_num])

            # loop through every asset
            for a in range(A):
                x = f_series_std[:, a, :] #N*F
                x = x @ pca_tranform[a]
                y = self.prev_log_return[:, a] #N*1
                lm.fit(x,y)
                # print("####### asset ", a, "#######")
                # print(lm.coef_)
                # print(lm.score(x,y))
                X_af[a] = lm.coef_

            # do allocation
            alloc = np.empty((A,),dtype=float)
            for a in range(A):
                pca_factors = factors[a] @ pca_tranform[a]
                signal = np.dot(pca_factors, X_af[a])
                alloc[a] = signal
        else:
            alloc = np.random.rand(A,)
        # print("Alloc", alloc)ß
        # load today's data as prev
        self.load_prev_data(price, factors)
        # assert price.shape[0] == factors.shape[0]
        return alloc
