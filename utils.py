# -*- coding: utf-8 -*-
"""
Created on Tue Aug 31 12:23:04 2021

@author: vivin
"""

import datetime
from os.path import isfile

class utils(object):
    @staticmethod 
    def compute_comparison_date(run_name, today, path, logger):
        logger.info('compare date not explicitly provided. computing based on 1d diff')
        prev_dt = today - datetime.timedelta(days=1)
        prev_dt_str = prev_dt.strftime("%Y-%m-%d")
        fn = '{}_{}'.format(run_name, prev_dt_str)
        tries = 0
        while not isfile('{}{}.csv'.format(path, fn)) and tries < 20:
            tries = tries + 1
            logger.warning('couldnt find compare file which is exactly 1w in the past. getting the last good file before that date')
            prev_dt = prev_dt - datetime.timedelta(days=1)
            prev_dt_str = prev_dt.strftime("%Y-%m-%d")
            fn = '{}_{}'.format(run_name, prev_dt_str)
        if tries == 20:
            logger.error('couldnt find appropriate comparison file. program is exiting')
            return None
        compare_date = prev_dt_str
        return compare_date