# -*- coding: utf-8 -*-
"""
Created on Wed Sep  1 20:28:55 2021

@author: vivin
"""
import argparse
import datetime
from clenow_calculator import CLENOW_CALCULATOR
from utils import utils
import pandas as pd
import numpy as np
import warnings
import sys

PATH = 'C:\\Users\\vivin\\Documents\\data\\momentum_clenow\\'
INCLUDE_OVERRIDE = ['JKPAPER.NS', 'ZENSARTECH.NS']

def _get_prev_realized_positions(run_name, today, path, logger):
    #capital not explicitly passed. calculate from previos portfolio
    prev_date = utils.compute_comparison_date(run_name, today, path, logger)
    fn_prev = '{}_{}'.format(run_name, prev_date)
    df_prev = pd.read_csv("{}{}.csv".format(PATH, fn_prev))
    df_prev.set_index('ticker', inplace=True)
    if 'Realized Price' not in df_prev.columns.values:
        logger.error('Realized Price column not present in the comparison file. will not be able to compute current capital. populate these cols')
        sys.exit(1)
    df_prev_positions = df_prev[~np.isnan(df_prev['Realized Price'])]
    return df_prev_positions['Realized Price'], df_prev_positions['Shares']    

def main(
        run_name='default',
        start=None,
        end=None,
        capital=1000000,
        extra_capital=None,
        avg_move_per_name=0.001,
        max_gap=0.15,
        exit_thresh=0.2,
        window_reg=90,
        window_trend=100,
        window_atr=20,
    ):
    try:
        warnings.filterwarnings("ignore")
        logger = utils.configure_logging(__name__, "{}\\logs\\clenow_calculator.log".format(PATH))
        today = datetime.datetime.today()
        today_str = today.strftime("%Y-%m-%d")
        end = end or today_str
        days_offset = int(max(window_reg, window_trend) * 1.5)
        start_offsetted = datetime.datetime.strptime(end, "%Y-%m-%d") - datetime.timedelta(days=days_offset)
        start = start or start_offsetted.strftime("%Y-%m-%d")
        file_name = "{}_{}".format(run_name, end)
        realized_prices, shares = _get_prev_realized_positions(run_name, today, PATH, logger)
            
        logger.info('Initializing the clenow calculator')
        calc = CLENOW_CALCULATOR(
            start, 
            end, 
            capital=capital,
            realized_prices=realized_prices,
            shares=shares,
            extra_capital=extra_capital,
            avg_move_per_name=avg_move_per_name, 
            max_gap=max_gap, 
            exit_thresh=exit_thresh, 
            window_reg=window_reg, 
            window_trend=window_trend, 
            window_atr=window_atr,
            include_override=INCLUDE_OVERRIDE,
            file_name=file_name,
            path=PATH,
            logger=logger
        )
        logger.info('calculator initialized. starting core calculations - regression and signals')
        calc.calc_regression_metrics()
        calc.calc_signals()
        logger.info('computing position sizes')
        calc.compute_position_sizes()
        logger.info('saving down the portfolio data')
        calc.pad_realized_values()
        calc.save_rebalanced_portfolio()
        sys.exit(0)
    except Exception as e:
        logger.warning('program has to be exited as an error was encountered: {}'.format(e))
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='parse arguments')
    parser.add_argument('--run_name', default='momentum-port-default')
    parser.add_argument('--start', default=None)
    parser.add_argument('--end', default=None)
    parser.add_argument('--capital', default=None)
    parser.add_argument('--extra_capital', default=None)
    parser.add_argument('--avg_move_per_name', default=0.001, type=float)
    parser.add_argument('--max_gap', default=0.15, type=float)
    parser.add_argument('--exit_thresh', default=0.2, type=float)
    parser.add_argument('--window_reg', default=90, type=int)
    parser.add_argument('--window_trend', default=100, type=int)
    parser.add_argument('--window_atr', default=20, type=int)
    args = parser.parse_args()
    main(
        run_name=args.run_name,
        start=args.start,
        end=args.end,
        capital=args.capital,
        extra_capital=args.extra_capital,
        avg_move_per_name=args.avg_move_per_name,
        max_gap=args.max_gap,
        exit_thresh=args.exit_thresh,
        window_reg=args.window_reg,
        window_trend=args.window_trend,
        window_atr=args.window_atr
    )