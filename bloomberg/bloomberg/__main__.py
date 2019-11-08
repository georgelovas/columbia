#!/usr/bin/env python
'''
@author: George lovas
georgelovas@quantumsolutionsplus.com

Created on Oct 14, 2019
'''

import os
import sys
import inspect
import logging
import pdblp
import argparse
import pandas as pd
import datetime as dt
import bloombergReader as blp
from pytz import timezone
"""
This application reads market data from the bloomberg terminal installed on the local machine 
and writes the output to a csv or series of csv files in the designated directory.

The input for the program is an excel file with two worksheets defined, Tickers and Fields
The tickers worksheet should have a single column if tickers with the heading 'TIckers'
the Fields worksheet should have a single column of fields with the heading 'Fields'

The program is run as follows:

    __main__ -i [excel file path/filename] -s [startdate] -e [enddate: defaults to today] -o [output directory] -c [checkpoint]
    
enter __main__ without arguments to get a complete list of arguments.

if checkpoint is set and the program is interupted, it will resume from where it left off assuming all the parameters remain the same. 
If it is suspected that the last file created is corrupted or incomplete, delete the file from the output directory to regenerate.

"""

def run(args):
    try:
        xls = pd.ExcelFile(args.inputs)
        dfTickers = pd.read_excel(xls, sheet_name='Tickers', header=0, skipfooter=0).reset_index(drop=True)
        dfFields = pd.read_excel(xls, sheet_name='Fields', header=0, skipfooter=0).reset_index(drop=True)
        tickers = dfTickers.Tickers.tolist()
        fields = dfFields.Fields.tolist()

        records = blp.readBloombergData(tickers, fields, args.startdate, args.enddate, args.outdir, args.periodicity, args.tickerRange, args.checkpoint)
        logger.info('read %s records' % records)
    except Exception as ex:
        logger.error('error reading input file: %s\n%r' % (args.input, ex))

def initLogger():
    root = '/var/tmp/marketdata/' if 'linux' in sys.platform else '../data/output'
    fh = logging.FileHandler(root + 'marketdata-{:%Y-%m-%d-%H-%M-%S}.log'.format(dt.datetime.now()))
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
    parser.add_argument('-c', '--checkpoint', action='store_true', help='when set will checkpoint intermediate files')
    parser.add_argument('-e', '--enddate', help='end date defaults to today', default=None)
    parser.add_argument('-i', '--inputs', help='tickers and fields to extract', default='../../data/bloomberg-metadata.xlsx')
    parser.add_argument('-o', '--outdir', help='output director to write files', default='../../data/output1')
    parser.add_argument('-p', '--periodicity', help='periodicity for historical data, e.g., DAILY, MONTHLY', default='DAILY')
    parser.add_argument('-s', '--startdate', help='start date', default='19700101')
    parser.add_argument('-t', '--tickerRange', help='number of tickers to process per pass', type=int, default=2)

    args = parser.parse_args()

    args.startdate = ''
    ##args.enddate = '20191001'
    args.checkpoint = True
    args.tickerRange = 1
    if not args.enddate:
        args.enddate = dt.datetime.now().strftime('%Y%m%d')
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
    logger.info('location = %s %s' % ( __location__, sys.version))
    print(main(appName))
