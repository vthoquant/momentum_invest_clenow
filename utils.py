# -*- coding: utf-8 -*-
"""
Created on Tue Aug 31 12:23:04 2021

@author: vivin
"""

import datetime
from os.path import isfile
import logging

class utils(object):
    @staticmethod 
    def compute_comparison_date(run_name, today, path, logger):
        logger.info('compare date not explicitly provided. computing based on 1d diffs')
        prev_dt = today - datetime.timedelta(days=1)
        prev_dt_str = prev_dt.strftime("%Y-%m-%d")
        fn = '{}_{}'.format(run_name, prev_dt_str)
        tries = 0
        while not isfile('{}{}.csv'.format(path, fn)) and tries < 20:
            tries = tries + 1
            prev_dt = prev_dt - datetime.timedelta(days=1)
            prev_dt_str = prev_dt.strftime("%Y-%m-%d")
            fn = '{}_{}'.format(run_name, prev_dt_str)
        if tries == 20:
            logger.error('couldnt find appropriate comparison file. program is exiting')
            return None
        compare_date = prev_dt_str
        return compare_date
    
    @staticmethod 
    def configure_logging(logger_name=None, path_and_file=None):
        logger = logging.getLogger(logger_name if logger_name is not None else __name__)
        logger.setLevel(logging.DEBUG)
        # Format for our loglines
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # Setup console logging
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        # Setup file logging as well
        if path_and_file is not None:
            fh = logging.FileHandler(path_and_file, mode='w')
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(formatter)
            logger.addHandler(fh)
        return logger