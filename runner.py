# -*- coding: utf-8 -*-
"""
Created on Wed Sep  1 20:28:55 2021

@author: vivin
"""
import argparse
import datetime
from clenow_calculator import CLENOW_CALCULATOR

def main(
        run_name='default',
        start=None,
        end=None,
        capital=1000000,
        avg_move_per_name=0.001,
        max_gap=0.15,
        exit_thresh=0.2,
        window_reg=90,
        window_trend=100,
        window_atr=20,
    ):
    today = datetime.datetime.today().strftime("%Y-%m-%d")
    end = end or today
    days_offset = int(max(window_reg, window_trend) * 1.5)
    start_offsetted = datetime.datetime.strptime(end, "%Y-%m-%d") - datetime.timedelta(days=days_offset)
    start = start or start_offsetted.strftime("%Y-%m-%d")
    file_name = "{}_{}".format(run_name, end)
    calc = CLENOW_CALCULATOR(
        start, 
        end, 
        capital=capital, 
        avg_move_per_name=avg_move_per_name, 
        max_gap=max_gap, 
        exit_thresh=exit_thresh, 
        window_reg=window_reg, 
        window_trend=window_trend, 
        window_atr=window_atr,
        file_name=file_name
    )
    calc.calc_regression_metrics()
    calc.calc_signals()
    calc.compute_position_sizes()
    calc.save_rebalanced_portfolio()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='parse arguments')
    parser.add_argument('--run_name', default='momentum-port-default')
    parser.add_argument('--start', default=None)
    parser.add_argument('--end', default=None)
    parser.add_argument('--capital', default=1000000, type=int)
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
        avg_move_per_name=args.avg_move_per_name,
        max_gap=args.max_gap,
        exit_thresh=args.exit_thresh,
        window_reg=args.window_reg,
        window_trend=args.window_trend,
        window_atr=args.window_atr
    )