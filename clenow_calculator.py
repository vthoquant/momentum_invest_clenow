# -*- coding: utf-8 -*-
"""
Created on Tue Aug 31 18:12:33 2021

@author: vivin
"""
import yfinance as yf
import pandas_datareader.data as pdr
import pandas as pd
import numpy as np
from talib.abstract import EMA, ATR
from sklearn.linear_model import LinearRegression
from os.path import isfile

class CLENOW_CALCULATOR(object):
    TRADING_DAYS = 250
    pop_file_name = 'ind_nifty500list'
    path = 'C:\\Users\\vivin\\Documents\\data\\momentum_clenow\\'
    def __init__(self, start, end, capital=1000000, window_reg=90, window_trend=200, window_atr=14, tickers=None, market_symbol='NIFTY50.NS'):
        self.start = start
        self.end = end
        self.capital = capital
        self.window_reg = window_reg
        self.window_trend = window_trend
        self.window_atr = window_atr
        self.tickers= tickers
        self.market_symbol = market_symbol
        self.data_adj_close = None
        self.data_high = None
        self.data_low = None
        self.data_close = None
        self.data_indicators = None
        self.regr_avg_ret = {}
        self.regr_r2 = {}
        self.regr_ovrl = {}
        self.load_data()
        self.compute_indicators()
        
    def load_data(self):
        if self.tickers is None:
            tickers_df = pd.read_csv("{}{}.csv".format(self.path, self.pop_file_name))
            tickers = tickers_df['Symbol']
            tickers = ["{}.NS".format(symb) for symb in tickers]
            self.tickers = tickers
        cache_file_path = "{}stocks_history_cache//{}.csv".format(self.path, self.end)
        if isfile(cache_file_path):
            full_data = pd.read_csv(cache_file_path, header=[0, 1], index_col=0)
            full_data.index = pd.to_datetime(full_data.index.values)
            self._get_failed_download_names(full_data)
        else:
            yf.pdr_override()
            full_data = pdr.get_data_yahoo(self.tickers, self.start, self.end)
            self._get_failed_download_names(full_data)

        full_data.fillna(method='bfill', inplace=True)
        self.data_adj_close = full_data.loc[:, 'Adj Close']
        self.data_high = full_data.loc[:, 'High']
        self.data_low = full_data.loc[:, 'Low']
        self.data_close = full_data.loc[:, 'Close']
        extra_cols = []
        for ticker in self.tickers:
            extra_cols.append('{} EMA'.format(ticker))
            extra_cols.append('{} ATR'.format(ticker))
            extra_cols.append('{} log'.format(ticker))
            
        self.data_indicators = pd.DataFrame(index=self.data_adj_close.index.values, columns=extra_cols)
        
    def _get_failed_download_names(self, data):
        data_adj_close = data.loc[:, 'Adj Close']
        data_adj_close_dropped = data_adj_close.dropna(how='all', axis=1)
        tickers_dropped = data_adj_close.columns.values[~np.isin(data_adj_close.columns.values, data_adj_close_dropped.columns.values)]
        for ticker in tickers_dropped:
            df = yf.download(ticker, self.start, self.end)
            for col in ['Adj Close', 'Close', 'Open', 'High', 'Low', 'Volume']:
                data.loc[:, (col, ticker)] = df.loc[:, col].reindex(data.index.values).values
        '''
        for ticker in self.tickers:
            check_col = "{} Adj Close".format(ticker)
            check_ser = data.loc[:, check_col]
            if len(check_ser.dropna())
        '''
        
    def compute_indicators(self):
        for ticker in self.tickers:
            self.data_indicators.loc[:, '{} EMA'.format(ticker)] = EMA(self.data_adj_close[ticker], timeperiod=self.window_trend)
            self.data_indicators.loc[:, '{} ATR'.format(ticker)] = ATR(self.data_high[ticker], self.data_low[ticker], self.data_close[ticker], timeperiod=self.window_atr)
            self.data_indicators.loc[:, '{} log'.format(ticker)] = np.log(self.data_adj_close[ticker])
            
    def calc_regression_metrics(self):
        for ticker in self.tickers:
            data_y = self.data_indicators.iloc[-self.window_reg:]['{} log'.format(ticker)]
            data_y = data_y.fillna(method='ffill')
            data_X = np.arange(0, len(data_y)).reshape(-1,1)
            reg_out = LinearRegression().fit(data_X, data_y)
            self.regr_avg_ret[ticker] = ((1 + reg_out.coef_[0]) ** self.TRADING_DAYS) - 1
            self.regr_r2[ticker] = reg_out.score(data_X, data_y)
            self.regr_ovrl[ticker] = self.regr_avg_ret[ticker] * self.regr_r2[ticker]
            