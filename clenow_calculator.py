# -*- coding: utf-8 -*-
"""
Created on Tue Aug 31 18:12:33 2021

@author: vivin
"""
import yfinance as yf
import pandas_datareader.data as pdr
import pandas as pd
import numpy as np
from talib.abstract import EMA, ATR, ROCP
from sklearn.linear_model import LinearRegression
from os.path import isfile, join
from os import listdir

class CLENOW_CALCULATOR(object):
    TRADING_DAYS = 250
    pop_file_name = 'ind_nifty500list'
    path = 'C:\\Users\\vivin\\Documents\\data\\momentum_clenow\\'
    stock_fundamentals_path = 'C:\\Users\\vivin\\Documents\\data\\my_portfolio\\'
    def __init__(self, start, end, capital=1000000, shares=None, realized_prices=None, extra_capital=None, avg_move_per_name=0.001, max_gap=0.15, exit_thresh=0.2, window_reg=90, window_trend=100, window_atr=20, tickers=None, bm_symbol='^NSEI', path=None, file_name=None, logger=None):
        self.start = start
        self.end = end
        self.shares = shares
        self.realized_prices = realized_prices
        self.avg_move_per_name = avg_move_per_name
        self.max_gap = 0.15
        self.exit_thresh = exit_thresh
        self.window_reg = window_reg
        self.window_trend = window_trend
        self.window_atr = window_atr
        self.tickers= tickers
        self.sectors = None if tickers is None else dict(zip(tickers, [None] *  len(tickers)))
        self.bm_symbol = bm_symbol
        self.path = path or self.path
        self.file_name = file_name or 'default'
        self.logger = logger
        self.bm_data_close = None
        self.data_adj_close = None
        self.data_high = None
        self.data_low = None
        self.data_close = None
        self.data_indicators = None
        self.regr_avg_ret = {}
        self.regr_r2 = {}
        self.regr_ovrl = {}
        self.signal_trend = {}
        self.signal_market_trend = None
        self.signal_top_thresh = {}
        self.signal_gap = {}
        self.signal_swot = {}
        self.signal_is_valid = {}
        self.prices_dict = {}
        self.atr_dict = {}
        self.position_table = None
        self.swot_data = None
        self.load_data()
        self.compute_capital_from_prev(capital, shares, extra_capital)
        self.load_swot_data()
        self.load_benchmark_data()
        self.compute_indicators()
        
    def compute_capital_from_prev(self, capital, shares, extra_capital):
        if capital is not None:
            self.capital=capital
        else:
            prices = self.data_adj_close.iloc[-1]
            prices = prices.reindex(shares.index.values)
            curr_portfolio_value = np.dot(prices, shares)
            total_value = curr_portfolio_value if extra_capital is None else curr_portfolio_value + extra_capital
            self.capital = total_value
        
    
    def load_data(self):
        if self.tickers is None:
            tickers_df = pd.read_csv("{}{}.csv".format(self.path, self.pop_file_name))
            tickers = tickers_df['Symbol']
            tickers = ["{}.NS".format(symb) for symb in tickers]
            self.tickers = tickers
            sectors = tickers_df['Industry']
            self.sectors = dict(zip(self.tickers, sectors))
        cache_file_path = "{}stocks_history_cache//{}.csv".format(self.path, self.end)
        if isfile(cache_file_path):
            full_data = pd.read_csv(cache_file_path, header=[0, 1], index_col=0)
            full_data.index = pd.to_datetime(full_data.index.values)
            failed_again = self._get_failed_download_names(full_data)
        else:
            yf.pdr_override()
            full_data = pdr.get_data_yahoo(self.tickers, self.start, self.end)
            failed_again = self._get_failed_download_names(full_data)
            full_data.to_csv(cache_file_path)

        log_failed_downloads = self.logger is not None and len(failed_again)
        if log_failed_downloads:
            self.logger.warning('failed downloading for {}. Please try re-running this program at a later time'.format(failed_again))
        full_data.fillna(method='bfill', inplace=True)
        full_data.fillna(method='ffill', inplace=True)
        if log_failed_downloads:
            self.logger.warning('padding the failed downloads with a price of 1.0 for all dates. make sure these names are ignored: {}'.format(failed_again))
        if len(failed_again):
            full_data.fillna(1.0, inplace=True)
        self.data_adj_close = full_data.loc[:, 'Adj Close']
        self.data_high = full_data.loc[:, 'High']
        self.data_low = full_data.loc[:, 'Low']
        self.data_close = full_data.loc[:, 'Close']
        extra_cols = []
        for ticker in self.tickers:
            extra_cols.append('{} EMA'.format(ticker))
            extra_cols.append('{} ATR'.format(ticker))
            extra_cols.append('{} ROCP'.format(ticker))
            extra_cols.append('{} Gap'.format(ticker))
            extra_cols.append('{} log'.format(ticker))
            
        self.data_indicators = pd.DataFrame(index=self.data_adj_close.index.values, columns=extra_cols)
        
    def load_benchmark_data(self):
        cache_file_path = "{}stocks_history_cache//bm_{}.csv".format(self.path, self.end)
        if isfile(cache_file_path):
            full_data = pd.read_csv(cache_file_path, index_col=0)
            full_data.index = pd.to_datetime(full_data.index.values)
        else:
            full_data = yf.download(self.bm_symbol, self.start, self.end)
            full_data.to_csv(cache_file_path)
            
        self.bm_data_close = full_data
        self.bm_data_close['BM EMA'] = EMA(self.bm_data_close['Adj Close'], timeperiod=self.window_trend)
        
    def _get_failed_download_names(self, data):
        data_adj_close = data.loc[:, 'Adj Close']
        data_adj_close_dropped = data_adj_close.dropna(how='all', axis=1)
        tickers_dropped = data_adj_close.columns.values[~np.isin(data_adj_close.columns.values, data_adj_close_dropped.columns.values)]
        failed_again = []
        for ticker in tickers_dropped:
            df = yf.download(ticker, self.start, self.end)
            if len(df) == 0:
                failed_again.append(ticker)
            for col in ['Adj Close', 'Close', 'Open', 'High', 'Low', 'Volume']:
                data.loc[:, (col, ticker)] = df.loc[:, col].reindex(data.index.values).values
                
        return failed_again
        
    def compute_indicators(self):
        for ticker in self.tickers:
            self.data_indicators.loc[:, '{} EMA'.format(ticker)] = EMA(self.data_adj_close[ticker], timeperiod=self.window_trend)
            self.data_indicators.loc[:, '{} ATR'.format(ticker)] = ATR(self.data_high[ticker], self.data_low[ticker], self.data_close[ticker], timeperiod=self.window_atr)
            self.data_indicators.loc[:, '{} ROCP'.format(ticker)] = ROCP(self.data_close[ticker], timeperiod=1)
            self.data_indicators.loc[:, '{} Gap'.format(ticker)] = np.where(np.abs(self.data_indicators['{} ROCP'.format(ticker)]) > self.max_gap, True, False)
            self.data_indicators.loc[:, '{} log'.format(ticker)] = np.log(self.data_adj_close[ticker])
            
    def calc_regression_metrics(self):
        for ticker in self.tickers:
            data_y = self.data_indicators.iloc[-self.window_reg:]['{} log'.format(ticker)]
            data_X = np.arange(0, len(data_y)).reshape(-1,1)
            reg_out = LinearRegression().fit(data_X, data_y)
            self.regr_avg_ret[ticker] = ((1 + reg_out.coef_[0]) ** self.TRADING_DAYS) - 1
            self.regr_r2[ticker] = reg_out.score(data_X, data_y)
            self.regr_ovrl[ticker] = self.regr_avg_ret[ticker] * self.regr_r2[ticker]
            
    def calc_signals(self):
        self.calc_swot_signals()
        last_data_adj_close = self.data_adj_close.iloc[-1].to_dict()
        last_data_indicators = self.data_indicators.iloc[-1].to_dict()
        last_data_bm_close = self.bm_data_close.iloc[-1].to_dict()
        self.position_table = pd.DataFrame(data={'ticker': list(self.regr_ovrl.keys()), 'Momentum Score': list(self.regr_ovrl.values())})
        self.position_table['mom_rank'] = self.position_table['Momentum Score'].rank(ascending=False)
        self.position_table.set_index('ticker', inplace=True)
        rank_thresh = int(len(self.position_table) * self.exit_thresh)
        self.position_table['include_by_rank'] = np.where(self.position_table['mom_rank'] <= rank_thresh, True, False)
        self.signal_top_thresh = self.position_table['include_by_rank'].to_dict()
        self.position_table.drop(columns=['include_by_rank'], inplace=True)
        self.signal_market_trend = True if last_data_bm_close['Adj Close'] > last_data_bm_close['BM EMA'] else False
        for ticker in self.tickers:
            self.signal_trend[ticker] = True if last_data_adj_close[ticker] > last_data_indicators['{} EMA'.format(ticker)] else False
            self.signal_gap[ticker] = np.max(self.data_indicators.iloc[-self.window_reg:]['{} Gap'.format(ticker)])
            self.signal_is_valid[ticker] = self.signal_trend[ticker] * (not self.signal_gap[ticker]) * self.signal_top_thresh[ticker] * (np.isnan(self.signal_swot[ticker]) or self.signal_swot[ticker])
            self.prices_dict[ticker] = last_data_adj_close[ticker]
            self.atr_dict[ticker] = last_data_indicators['{} ATR'.format(ticker)]
    
    
    def calc_swot_signals(self):
        swot_df = self.swot_data.reindex(self.tickers)
        swot_df_nan = swot_df[np.isnan(swot_df['swot.ovrl_score'])]
        swot_df_nonan = swot_df[~np.isnan(swot_df['swot.ovrl_score'])]
        swot_df_nonan['isSwotOK'] = swot_df_nonan['swot.ovrl_score'] >= 0.5
        swot_df_nonan['isOverallOK'] = swot_df['mci.overall'].isin(['Mid range performer', 'Mid-range performer', 'Strong Performer', 'Unrated'])
        swot_df_nonan['swot_signal'] = swot_df_nonan['isSwotOK'] & swot_df_nonan['isOverallOK']
        swot_df_nonan.drop(columns=['isSwotOK', 'isOverallOK'], inplace=True)
        swot_df_nan['swot_signal'] = np.nan
        swot_df = pd.concat([swot_df_nonan, swot_df_nan])
        self.signal_swot = swot_df['swot_signal'].to_dict()
    
    def load_swot_data(self):
        all_files = [f for f in listdir(self.stock_fundamentals_path) if isfile(join(self.stock_fundamentals_path, f))]
        all_available_sectors = [f.split(".")[0] for f in all_files if '.html' in f]
        df_arr = []
        for sector in all_available_sectors:
            df_temp = pd.read_csv("{}{}.csv".format(self.stock_fundamentals_path, sector))
            df_arr.append(df_temp)
        df = pd.concat(df_arr, axis=0)
        df.drop_duplicates('ticker', inplace=True)
        df.set_index('ticker', inplace=True)
        self.swot_data = df[['mci.overall', 'mci.pio_score', 'swot.ovrl_score']]
    
            
    def compute_position_sizes(self):
        sector_df = pd.DataFrame(data={'ticker': list(self.sectors.keys()), 'Sector': list(self.sectors.values())})
        sector_df.set_index('ticker', inplace=True)
        price_df = pd.DataFrame(data={'ticker': list(self.prices_dict.keys()), 'Price': list(self.prices_dict.values())})
        price_df.set_index('ticker', inplace=True)
        signal_trend_df = pd.DataFrame(data={'ticker': list(self.signal_trend.keys()), 'isUpTrend': list(self.signal_trend.values())})
        signal_gap_df = pd.DataFrame(data={'ticker': list(self.signal_gap.keys()), 'isGap': list(self.signal_gap.values())})
        signal_top_df = pd.DataFrame(data={'ticker': list(self.signal_top_thresh.keys()), 'isTopPerc': list(self.signal_top_thresh.values())})
        signal_swot_df = pd.DataFrame(data={'ticker': list(self.signal_swot.keys()), 'isSwotOK': list(self.signal_swot.values())})
        signal_trend_df.set_index('ticker', inplace=True)
        signal_gap_df.set_index('ticker', inplace=True)
        signal_top_df.set_index('ticker', inplace=True)
        signal_swot_df.set_index('ticker', inplace=True)
        validity_df = pd.DataFrame(data={'ticker': list(self.signal_is_valid.keys()), 'isValid': list(self.signal_is_valid.values())})
        validity_df.set_index('ticker', inplace=True)
        atr_df = pd.DataFrame(data={'ticker': list(self.atr_dict.keys()), 'ATR': list(self.atr_dict.values())})
        atr_df.set_index('ticker', inplace=True)
        self.position_table = pd.concat([self.position_table, sector_df, price_df, signal_trend_df, signal_gap_df, signal_top_df, signal_swot_df, validity_df, atr_df], axis=1)
        self.position_table['Shares Raw'] = np.floor((self.capital * self.avg_move_per_name)/self.position_table['ATR'])
        self.position_table['Shares'] = np.where(self.position_table['isValid'], self.position_table['Shares Raw'], 0.0)
        self.position_table['Allocation'] = self.position_table['Shares'] * self.position_table['Price']
        self.position_table['Allocation %'] = self.position_table['Allocation']/self.capital
        self.position_table.sort_values(by='mom_rank', ascending=True, inplace=True)
        self.position_table['Allocation % cumul'] = self.position_table['Allocation %'].cumsum()
        
    def pad_realized_values(self):
        if self.shares is not None:
            df_realized = pd.DataFrame(index=self.shares.index.values, columns=['Realized Price', 'Realized Value', 'Cumul Portfolio Value', 'Cumul Portoflio %'])
            df_realized.loc[:, 'Realized Price'] = self.realized_prices
            df_realized = df_realized.reindex(self.position_table.index.values)
            df_realized.index.name = 'ticker'
            self.position_table = pd.concat([self.position_table, df_realized], axis=1)
        
    def save_rebalanced_portfolio(self):
        full_path = "{}{}.csv".format(self.path, self.file_name)
        self.position_table.to_csv(full_path)
            