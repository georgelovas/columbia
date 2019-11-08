#!/usr/bin/env python
'''
@author: George lovas
georgelovas@quantumsolutionsplus.com

Created on Oct 31, 2019
'''

import os
import sys
import inspect
import logging
import argparse
import pandas as pd
import datetime as dt
import analytics
from pytz import timezone

"""
This application runs a series of machine learning algorithms on a variety of data slices 
extracted from a kdb database
"""

def run(args):
    try:
        status = analytics.performAnalysis(args)
        logger.info('completed analysis %s' % status)
    except Exception as ex:
        logger.error('error performing analysis\n%r' % ex)

def initLogger():
    root = '/var/tmp/analysis/' if 'linux' in sys.platform else '../data/output'
    fh = logging.FileHandler(root + 'analysis-{:%Y-%m-%d-%H-%M-%S}.log'.format(dt.datetime.now()))
    formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(logging.StreamHandler())

def main(appname):
    ####################################################################################################################
    # set interpreter options for debugging
    ####################################################################################################################
    pd.set_option('display.max_rows', 5000)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 2000)
    pd.set_option('precision', 2)
    pd.set_option('display.float_format', lambda x: '%.8f' % x)

    ####################################################################################################################
    # set today as default date
    ####################################################################################################################
    asofdate = dt.datetime.now().astimezone(timezone('US/Eastern'))

    ####################################################################################################################
    # process command line args
    ####################################################################################################################
    parser = argparse.ArgumentParser(prog='BloombergReader')
    parser.add_argument('-i', '--inputs', help='tickers and fields to extract from spreadsheet instead of database',default=None)
    parser.add_argument('-o', '--outdir', help='output director to write files', default='../../data/output1')
    parser.add_argument('-qp', '--qpath', help='path to q executable')
    parser.add_argument('-qi', '--qinstance', help='for debugging a running qinstance host:port', default=None)

    args = parser.parse_args()

    if not args.outdir:
        args.outdir = os.path.join(os.getcwd(), '..', '../../data/output')

    ################################################################################################################
    # run the program
    ################################################################################################################

    return run(args)

if __name__ == '__main__':
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    appName = os.path.basename(inspect.getfile(inspect.currentframe()))
    logger = logging.getLogger()
    initLogger()
    logger.info('location = %s %s' % (__location__, sys.version))
    print(main(appName))
