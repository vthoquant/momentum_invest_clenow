# -*- coding: utf-8 -*-
"""
Created on Mon Sep  6 12:38:44 2021

@author: vivin
"""
import argparse
import datetime
from utils import utils
import warnings
import sys
import pandas as pd
import numpy as np

PATH = 'C:\\Users\\vivin\\Documents\\data\\momentum_clenow\\'
SHARES_DIFF_THRESH = 0.2

def _get_index_disqualified(df_prev, df_curr):
    prev_names = df_prev.index.values
    curr_names = df_curr.index.values
    index_disql = prev_names[~np.isin(prev_names, curr_names)]
    return df_prev[index_disql] if len(index_disql) else pd.DataFrame()

def _get_criteria_disqualified(df_prev, df_curr, logger):
    prev_positions = df_prev[~np.isnan(df_prev['Realized Price'])]
    #sanity checks
    all_valid = np.min(prev_positions['isValid'])
    if not all_valid:
        logger.warning("seems like the prevoius portfolio has a position whcih was not classified as valid at that time. please check!")
    else:
        logger.info("all previous positions were valid as of that date")
    disqualified_df = df_curr[~df_curr['isValid']].reindex(prev_positions.index.values).dropna(how='all', axis=0)
    return disqualified_df

def _get_rebalanced_positions_from_prev(df_prev, df_curr):
    prev_positions = df_prev[~np.isnan(df_prev['Realized Price'])][['Realized Price', 'Shares']]
    prev_positions.rename(columns={'Shares': 'Shares_prev'}, inplace=True)
    curr_positions = df_curr.reindex(prev_positions.index.values)[['Price', 'Shares']]
    curr_positions = pd.concat([prev_positions, curr_positions], axis=1)
    curr_positions['Position_rebalance_req'] = np.where(np.abs(curr_positions['Shares'] - curr_positions['Shares_prev'])/curr_positions['Shares_prev'] > SHARES_DIFF_THRESH, True, False)
    return curr_positions

def main(
        run_name='default',
        run_date=None,
        compare_date=None
    ):
    try:
        warnings.filterwarnings("ignore")
        logger = utils.configure_logging(__name__, "{}\\logs\\portoflio_rebalance.log".format(PATH))
        today = datetime.datetime.today()
        today_str = today.strftime("%Y-%m-%d")
        run_date = run_date or today_str
        if compare_date is None:
            compare_date = utils.compute_comparison_date(run_name, today, PATH, logger)
        if compare_date is None:
            sys.exit(1)
          
        fn_curr = '{}_{}'.format(run_name, run_date)
        df_curr = pd.read_csv("{}{}.csv".format(PATH, fn_curr))
        fn_prev = '{}_{}'.format(run_name, compare_date)
        df_prev = pd.read_csv("{}{}.csv".format(PATH, fn_prev))
        df_curr.set_index('ticker', inplace=True)
        df_prev.set_index('ticker', inplace=True)
        if 'Realized Price' not in df_prev.columns.values:
            logger.error('Realized Price column not present in the comparison file. will not be able to run the comparison effectively. populate these cols')
            sys.exit(1)
        index_disqualified_df = _get_index_disqualified(df_prev, df_curr)
        criteria_disqualified_df = _get_criteria_disqualified(df_prev, df_curr, logger)
        position_rebal_df = _get_rebalanced_positions_from_prev(df_prev, df_curr)
        with pd.ExcelWriter("{}{}_{}_diffs.xlsx".format(PATH, run_name, run_date)) as writer:
            index_disqualified_df.to_excel(writer, sheet_name='Index disqualified')
            criteria_disqualified_df.to_excel(writer, sheet_name='Criteria disqualified')
            position_rebal_df.to_excel(writer, sheet_name='Poaition rebalance requirements')
        
        sys.exit(0)
    except Exception as e:
        logger.warning('program has to be exited as an error was encountered: {}'.format(e))
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='parse arguments')
    parser.add_argument('--run_name', default='momentum-port-default')
    parser.add_argument('--run_date', default=None)
    parser.add_argument('--compare_date', default=None)
    args = parser.parse_args()
    main(
        run_name=args.run_name,
        run_date=args.run_date,
        compare_date=args.compare_date,
    )