# columbia
project for Columbia Business School
download or fork bloomberg/bloombergReader.py from https://github.com/georgelovas/columbia.git


[records | dataFrame] = readBloombergData(startdate, enddate, tickers, fields, output=None, periodicity='DAILY', tickerRange=None, checkpoint=False)

startdate - date range start 2019-01-01 or 209190101 string format
enddate - date range end in 2019-01-01 or 209190101 string format
tickers - an array of tickers
fields - an array of fields
output - output file or directory; if not ending in .csv, considered directory; will create if not exists; default filename = marketdata.csv
periodicity - one of DAILY or MONTHLY; default is DAILY
tickerRange - for large number of tickers limit query to the block defined by the range
checkpoint - for large data request, set to true to create series of output files.  Progeram may be terminated and restarted at last checkpoint 
records - number of records processed
dataframe = dataframe with four columns, Date, Ticker, Field, Value

Examples...

Example 1
The following example will download the data for the tickers, fields for the date range

import bloombergReader as blp

tickers = ['MAT US EQUITY', 'MPC US EQUITY', 'ACGIX US EQUITY']
fields = ['PX_LAST', 'PX_VOLUME', 'PX_HIGH', 'PX_LOW', 'CUR_MKT_CAP']
startdate = '20190101'
enddate = '20191027'

df = blp.readBloombergData(tickers, fields, startdate, enddate)
df.head()
df.shape


Example 2
The following example will download the data for the tickers, fields for the date range and output to the file sbown

import bloombergReader as blp

tickers = ['MAT US EQUITY', 'MPC US EQUITY', 'ACGIX US EQUITY']
fields = ['PX_LAST', 'PX_VOLUME', 'PX_HIGH', 'PX_LOW', 'CUR_MKT_CAP']
startdate = '20190101'
enddate = '20191027'
output = 'C:/marketdata/mymarketdata.csv'

records = blp.readBloombergData(tickers, fields, startdate, enddate, output)

example 3
The following example will download the data for the tickers, fields for the date range and output to a series of files in the directory shown

import bloombergReader as blp

tickers = ['MAT US EQUITY', 'MPC US EQUITY', 'ACGIX US EQUITY']
fields = ['PX_LAST', 'PX_VOLUME', 'PX_HIGH', 'PX_LOW', 'CUR_MKT_CAP']
startdate = '20190101'
enddate = '20191027'
output = 'C:/tmp/example3'

records = blp.readBloombergData(tickers, fields, startdate, enddate, output, tickerRange=1, checkpoint=True)

example4
This example will return real time data for the tickers and fields shown

import bloombergReader as blp

tickers = ['MAT US EQUITY', 'MPC US EQUITY', 'ACGIX US EQUITY']
fields = ['PX_LAST', 'PX_VOLUME', 'PX_HIGH', 'PX_LOW', 'CUR_MKT_CAP']

df = blp.readBloombergData(tickers, fields)

