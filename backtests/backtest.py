from __future__ import (absolute_import, division, print_function, unicode_literals)
import pandas as pd
import numpy as np
import logging
import datetime
import os.path
import sys
from pytz import timezone
from scipy.stats import mstats

import backtrader as bt
import backtrader.analyzers as btanalyzers
import matplotlib.pyplot as plt


#global variables
SHOW_BACKTRADER_CHARTS = False

# Structure to store daily values
PNL_DICT = {}
MKT_DICT = {}

# enable winsorization
WINSORIZATION_LEVEL = 1000

# control the output
DEBUG = True
SAVE_OUTPUT_FILES = False

# control the tickers to include
NUM_TICKERS = 1
TICKERS = None #['x189']

# initial cash balance
START_BALANCE = 1000000.0

class MySignal(bt.Strategy):
    params = (('tradesizeadj', 1),)
    lines = ('signal',)
    global PNL_DICT, MKT_DICT

    def __init__(self):
        self.lines.signal = self.data.openinterest
        self.order = None

    def updateDict(self, dict, date, ticker, value):
        if ticker in dict.keys():
            dict[ticker].append((date, value))
        else:
            dict[ticker] = [(date, value)]

    def next(self):
        idx = self.datetime.idx
        dt = self.datetime.date()
        cash = self.broker.get_cash()
        self.updateDict(MKT_DICT, idx, 'cash', cash)
        # logger.info('processing data for %s row: %s  cash balance: %.2f' % (dt, idx, cash))
        for i, d in enumerate(self.datas):
            dn = d._name
            pos = self.getposition(d).size
            signal = d.openinterest.array[idx]
            size = 0
            if pos:
                self.updateDict(MKT_DICT, idx, dn, pos * d.close.array[idx])
                if signal > 0:
                    prev_signal = d.openinterest.array[idx-1]
                    if signal < prev_signal:
                        # reduce the size...
                        size = int(round(pos * (1 - signal/prev_signal)))
                        if size > 10:
                            # logger.info('Position: %s' % pos)
                            self.order = self.sell(data=d, size=size, price=d.close[0])
                    elif signal > prev_signal:
                        # increase size
                        size = int(self.sizer.getsizing(d, True) * signal)
                        if size > 10:
                            # logger.info('Position: %s' % pos)
                            self.order = self.buy(data=d, size=size, price=d.close[0])
                else:
                    self.order = self.close(data=d)
            elif signal > 0:
                try:
                    size = int(self.sizer.getsizing(d, True) * signal)
                    self.order = self.buy(data=d, size=size) #, price=d.close[0])
                    #self.updateDict(MKT_DICT, idx, dn, size * d.close.array[idx])
                except Exception as ex:
                    logger.info('Exception trading: %s  row: %s' % (dn, idx))
                    pass

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                if DEBUG:
                    logger.info('BUY EXECUTED, row: %s, Ticker: %s, Price: %.2f, Shares: %s, Cost: %.2f, Comm %.2f, Cash %.2f' %
                          (order.plen, order.data._name, order.executed.price, order.size, order.executed.value, order.executed.comm, self.broker.getcash()))
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                if DEBUG:
                    logger.info('SELL EXECUTED, row %s, Ticker: %s, Price: %.2f, shares: %s, Cost: %.2f, Comm %.2f, Cash %.2f' % (
                        order.plen, order.data._name, order.executed.price, order.size, order.executed.value, order.executed.comm, self.broker.getcash()))
                    pass
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            message = 'name %s idx: %s size: %s  price %s  cash: %s' % (order.data._name, order.plen, order.size, order.data.close[0], self.broker.getcash())
            if order.status == order.Canceled:
                logger.info('Order Margin exceeded: %s' % message)
            elif order.status == order.Rejected:
                logger.info('Order Margin exceeded: %s' % message)
            elif order.status == order.Margin:
                logger.info('Order Margin exceeded: %s' % message)

        # Write down: no pending order
        self.order = None

    def notify_trade(self, trade):
        if trade.isclosed:
            ticker = trade.getdataname()
            self.updateDict(PNL_DICT, trade.barclose, ticker, trade.pnlcomm)
        return

class MySizer(bt.sizers.PercentSizer):
    def _getsizing(self, comminfo, cash, data, isbuy):
        self.params.percents = 95
        self.params.retint = True
        price0 = data.close[0]
        try:
            price1 = data.close[1]
        except:
            price1 = price0
        price = max(price0, price1)
        size = cash / price * (self.params.percents / 100)
        if self.p.retint:
            size = int(size)
        return size

def convertReturnsToPrice(filename):
    df = pd.read_csv(filename)
    df['assetid'] = 'x' + df.assetid.astype(str)
    # to maintain same order as pivot sorts cols alphabetically
    order = df.assetid.unique()
    df = df.pivot(index='date', columns='assetid', values='value')
    df.index = pd.to_datetime(df.index)
    df = df.reindex(order, axis=1)
    if WINSORIZATION_LEVEL:
        df = winsorize(df)
    dfp = 100 + ((df + 1).cumprod() - 1) * 100
    return dfp

def getPrices(filename):
    df = convertReturnsToPrice(filename)
    # read the prices
    df = df.sort_index()
    df = df.ffill()
    # drop columns with bad prices
    droplist = ['x209', 'x291', 'x657', 'x1079', 'x2781', 'x2953', 'x3549', 'x3850', 'x4034', 'x5388',\
                'x11286','x11882', 'x12486', 'x14258', 'x18092', 'x19354', 'x19503', 'x19540','x67592',\
                'x751','x1493','x2028','x5431','x6103','x8532','x8547','x8667','x10746','x15703','x16990',\
                'x17237','x19467','x21145','x21757','x62268', 'x6240','x6217']

    for col in df.columns:
        distinct = df[col].unique()
        if len(distinct) == 1 and pd.isna(distinct[0]):
            droplist.append(col)
    if len(droplist):
        logger.info('dropping nan columns %s' % droplist)
        df = df.drop(droplist, axis=1)
    if TICKERS:
        df = df[TICKERS]
    elif NUM_TICKERS and NUM_TICKERS < len(df.columns):
        df = df[df.columns[0:NUM_TICKERS]]
    return df


def winsorize(df, limits=(0.01,0.01), inclusive=(False,False)):
    def winsorizeSeries(s):
        try:
            a = s[~pd.isnull(s)].astype(float)
            s[~pd.isnull(s)] = mstats.winsorize(a, limits)
        except Exception as ex:
            pass
        return s
        # df = df.apply(winsorizeSeries, axis=0)
    df = df.clip(upper=WINSORIZATION_LEVEL)
    return df

def getSignals(dfp, filename):
    #read the signals
    df = pd.read_csv(filename)
    df = df.set_index('date')
    df.index = pd.to_datetime(df.index)
    # reorder signal columns to match prices...
    df = df[dfp.columns]
    # shift columns up two periods...
    df = df.shift(periods=-1).ffill()
    # find any missing rows in signals data...
    missing = set(dfp.index) ^ set(df.index)
    if missing:
        # create empty dataframe
        dfm = pd.DataFrame([],index=missing, columns=df.columns)
        df = dfm.append(df)
    df = df.sort_index()
    df = df.fillna(0)
    if TICKERS:
        df = df[TICKERS]
    elif NUM_TICKERS and NUM_TICKERS < len(df.columns):
        df = df[df.columns[0:NUM_TICKERS]]
    return df

def computePriceWeightedSignals(dfp, dfs):
    # compute price weighted signals
    df = dfp.mask(dfs == 0, 0.0)
    df = df.div(df.sum(axis=1),axis=0).fillna(0.0)
    return df

def computeEqualWeightedSignals(dfs):
    sum = dfs.sum(axis=1)
    if any(sum > 1):
        dfs = dfs.div(sum, axis=0).fillna(0.0)
    return dfs

def addData(cerebro, dfp, dfs):
    for idx in range(len(dfp.columns)):
        col = dfp.columns[idx]
        # if col == 'x205':
        df = dfp[[col]].join(dfs[[col]],rsuffix='x').fillna(-1.0)
        # use close for the price and open for the signal
        df.columns = ['close', 'openinterest']
        df['open'] = df.close
        df['high'] = df.close
        df['low'] = df.close
        df['volume'] = df.close
        data = bt.feeds.PandasData(dataname=df, name=col)
        cerebro.adddata(data)
    logger.info('added %s columns' % (idx+1))

def genPnlTable(dfp, dict):
    df = pd.DataFrame()
    for key in dict.keys():
        dfi = pd.DataFrame(dict[key], columns=['index', key]).set_index('index')
        df = dfi if df.empty else df.join(dfi)
    df = dfp.reset_index()[['date']].join(df).set_index('date')
    ## to preserve column order
    cols = set(dfp.columns) & set(df.columns)
    order = ['cash']
    order.extend(dfp.columns[[x in cols for x in dfp.columns]])
    df = df.reindex(columns=order)
    return df

def initLogger():
    root = '/tmp/marketdata/' if 'linux' in sys.platform else '../log'
    os.makedirs(root, exist_ok=True)
    fh = logging.FileHandler(os.path.join(root,'backtest-{:%Y-%m-%d-%H-%M-%S}.log'.format(datetime.datetime.now())))
    formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    if not DEBUG:
        logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

def plotOutputResults(dfp, dfc, balance, strats):
    dfc['strat'] = dfc.fillna(0.0).sum(axis=1)

    ## df = dfp.mul(START_BALANCE / 100.0)[dfc.drop(['cash', 'strat'], axis=1).columns].join(dfc[['strat']]).ffill()

    # compute benchmark return by aggregating all returns
    # securities in the price file to get singular returns
    dfx = dfp.pct_change()
    # compute row weights to filter NAN values per row
    w = dfx.apply(lambda x: x.count(), axis=1)
    dfr = pd.DataFrame(dfx.div(w, axis=0).sum(axis=1), columns=['benchmark'])
    dfr = START_BALANCE + ((dfr + 1).cumprod() - 1) * START_BALANCE
    df = dfr.join(dfc[['strat']]).ffill()
    ax = df.plot(kind='line')

    # add stats...
    tickers = TICKERS if TICKERS else dfp.columns[0:NUM_TICKERS] if NUM_TICKERS else dfp.columns
    tickers = ','.join(tickers) if len(tickers) <= 5 else '%s - %s' % (tickers[0], tickers[-1])

    textstr = '\n'.join((
        'Tickers:    %s' % tickers,
        'Start Date  %s' % dfp.index[0].strftime('%m/%d/%Y'),
        'End Date:   %s' % dfp.index[-1].strftime('%m/%d/%Y'),
        'AUM Start:  $%.0fMM' % (START_BALANCE/1E6),
        'AUM End:    $%.0fMM' % (balance/1E6),
        'Sharpe:     %.2f' % strats[0].analyzers.mysharpe.get_analysis()['sharperatio']))

    props = dict(boxstyle='round', facecolor='dodgerblue', alpha=0.5)
    ax.text(0.1, 0.65, textstr, transform=ax.transAxes, family='monospace', fontsize=8, verticalalignment='top', bbox=props)
    plt.legend(loc='upper left')
    plt.show()

def main():
    logger.info('Starting Backtest program')
    showChart = True
    cerebro = bt.Cerebro()
    cerebro.addstrategy(MySignal)
    #cerebro.addsizer(bt.sizers.PercentSizer)
    #cerebro.addsizer(bt.sizers.AllInSizer)
    cerebro.addsizer(MySizer)
    cerebro.addanalyzer(btanalyzers.SharpeRatio, _name='mysharpe')
    cerebro.broker.setcommission(commission=0.001)
    cerebro._disable_runonce()

    dfp = getPrices('../arabesque/DailyAssetReturns.csv')
    dfs = getSignals(dfp, '../arabesque/IEOR4576_ALLOC.csv')
    #dfx = computePriceWeightedSignals(dfp, dfs)
    dfs = computeEqualWeightedSignals(dfs)

    addData(cerebro, dfp, dfs)

    cerebro.broker.setcash(1000000.0)
    logger.info('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    strats = cerebro.run()
    logger.info('Sharpe Ratio: %s' % strats[0].analyzers.mysharpe.get_analysis()['sharperatio'])
    logger.info('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    dfc = genPnlTable(dfp, MKT_DICT)

    if SAVE_OUTPUT_FILES:
        dft = genPnlTable(dfp, PNL_DICT)
        dft.to_csv('../arabesque/backtest-trade-pnl.csv')
        dfc.to_csv('../arabesque/backtest-daily-pnl.csv')

    if not SHOW_BACKTRADER_CHARTS:
        plotOutputResults(dfp, dfc, cerebro.broker.getvalue(), strats)
    else:
        pkwargs = dict(style='bar')
        cerebro.plot(**pkwargs)
if __name__ == '__main__':
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    logger = logging.getLogger()
    initLogger()
    logger.info('location = %s %s' % (__location__, sys.version))
    main()
