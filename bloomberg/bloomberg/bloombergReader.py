#!/usr/bin/env python
'''
Created on Oct 27, 2019

@author: george lovas
         georgelovas@quantumsolutionsplus.com
         Quantum Solutions Plus, LLC

'''
import os
import blpapi
import logging.config
import pandas as pd
import datetime as dt
import calendar
import glob

logger = logging.getLogger(__name__)

SECURITY_DATA = blpapi.Name("securityData")
SECURITY = blpapi.Name("security")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
ERROR_INFO = blpapi.Name("errorInfo")
BLP_FIELD_LIMIT = 24
header = True

##############################################################################################
# Program Entry Point
##############################################################################################

def readBloombergData(tickers, fields, startdate=None, enddate=None, output=None, periodicity='DAILY', tickerRange=None, checkpoint=False):
    records = 0
    try:
        ################################################################################################################
        # open bloomberg session...
        ################################################################################################################
        def chunks(l, n):
            for idx in range(0, len(l), n):
                yield l[idx:idx + n]
        if not tickerRange:
            tickerRange = len(tickers)

        if checkpoint and output:
            ##if program interuppted restart from where left off...
            path = output
            if output.endswith('.csv'):
                path = os.path.dirname(output)
            if os.path.exists(path):
                try:
                    files = glob.glob('%s/*.csv' % output)
                    latest = max(files, key=os.path.getctime)
                    df = pd.read_csv(latest)
                    endticker = df.iloc[-1,2]
                    idx = tickers.index(endticker) + 1
                    tickers = tickers[idx:]
                    header = False
                except Exception as ex:
                    pass
        ## start bloomberg session
        logger.info('Started Reading Bloomberg data')
        sessionOptions = blpapi.SessionOptions()
        # sessionOptions.setServerHost('localhost')
        sessionOptions.setServerHost('127.0.0.1')
        sessionOptions.setServerPort(8194)
        session = blpapi.Session(sessionOptions)
        mktDataAr = []
        if session.start():
            if session.openService('//blp/refdata'):
                ## do 25 fields at a time... bloomberg limit
                for tickerList in chunks(tickers, tickerRange):
                    for fieldList in chunks(fields, BLP_FIELD_LIMIT):
                        if startdate:
                            subArray = GetHistoricalData(session, startdate, enddate, tickerList, fieldList, periodicity)
                        else:
                            subArray = GetIntradayData(session, tickerList, fieldList)
                        records += len(subArray)
                        mktDataAr.extend(subArray)
                        logger.info("read %s records from Bloomberg..." % records)
                    if checkpoint and output:
                        writeMarketData(mktDataAr, output, checkpoint)
                        mktDataAr = []
            else:
                logger.exception("Failed to open bloomberg service...")
        else:
            logger.exception("Failed to start bloomberg session...")
    except Exception as ex:
        logger.exception("Failed to execute...", ex)
    finally:
        ret = records
        if not checkpoint:
            if output:
                writeMarketData(mktDataAr, output, checkpoint)
            else:
                columns = ['Date', 'Ticker', 'Field', 'Value']
                ret = pd.DataFrame(mktDataAr, columns=columns)
        if session:
            session.stop()
        return ret

##############################################################################################
# Get Daily Marketdata
##############################################################################################
def GetIntradayData(session, tickers, fields):
    try:
        service = session.getService('//blp/refdata')
        request = service.createRequest("ReferenceDataRequest")

        ###########################################################
        # add tickers to the request
        ###########################################################
        for ticker in tickers:
            request.append('securities', ticker)
        ###########################################################
        # add field to the request
        ###########################################################
        fields = ['LAST_UPDATE_DT'] + fields
        for field in fields:
            request.append('fields', field)
        cid = session.sendRequest(request)
        ###########################################################
        # loop through response and add to array
        #
        # Realtime response is structured with array of securities
        # each with a fieldData structure
        ###########################################################
        mktDataAr = []
        while (True):
            try:
                ev = session.nextEvent(500)
                for msg in ev:
                    if cid in msg.correlationIds():
                        securityDataArray = msg.getElement(SECURITY_DATA)
                        for securityData in securityDataArray.values():
                            try:
                                ###########################################################
                                # date field must be first in list
                                ###########################################################
                                dateField = None
                                ticker = securityData.getElementAsString(SECURITY)
                                fieldData = securityData.getElement(FIELD_DATA)
                                for field in fields:
                                    if fieldData.hasElement(field, True):
                                        fieldValue = fieldData.getElementValue(field)
                                        if not dateField:
                                            if isinstance(fieldValue, dt.date):
                                                dateField = fieldValue
                                            elif isinstance(fieldValue, dt.time):
                                                dateField = dt.datetime.combine(dt.datetime.now().date(), fieldValue)
                                            else:
                                                logger.error("Error: missing date field %s: %s: %r" % (ticker, field))
                                        else:
                                            ###########################################################
                                            # transpose data to secId, fieldId, value, date
                                            ###########################################################
                                            # convert date fields to epoch
                                            if isinstance(fieldValue, dt.date):
                                                fieldValue = calendar.timegm(fieldValue.timetuple())
                                            rowData = []
                                            rowData.append(dateField)
                                            rowData.append(ticker)
                                            rowData.append(field)
                                            rowData.append(fieldValue)
                                            mktDataAr.append(rowData)
                            except Exception as ex:
                                logger.exception("Error Reading data for %s: %r" % (ticker, ex))
                if ev.eventType() == blpapi.Event.RESPONSE:
                    break
            except Exception as ex:
                logger.exception("Error Reading Bloomberg event data: %r" % ex)
    except Exception as ex:
        logger.exception("Error Reading Bloomberg data: %r" % ex)
    else:
        return mktDataAr

##############################################################################################
# Get Historical Marketdata
##############################################################################################
def GetHistoricalData(session, startdate, enddate, tickers, fields, periodicity):
    try:
        service = session.getService('//blp/refdata')
        ################################################################################################################
        # add start and end date for historical data
        ################################################################################################################
        request = service.createRequest("HistoricalDataRequest")
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", periodicity.upper())
        startDate = pd.to_datetime(startdate).strftime('%Y%m%d')
        endDate = pd.to_datetime(enddate).strftime('%Y%m%d')
        request.set("startDate", startDate)
        request.set("endDate", endDate)
        # request.set("maxDataPoints", 100)

        ###########################################################
        # add tickers to the request
        ###########################################################
        for ticker in tickers:
            request.append('securities', ticker)
        ###########################################################
        # add field to the request
        ###########################################################
        for field in fields:
            request.append('fields', field)

        ###########################################################
        # loop through response and add to array
        #
        # Historical response is structured with array of fieldData
        # for each security
        ###########################################################
        cid = session.sendRequest(request)
        mktDataAr = []
        while (True):
            try:
                ev = session.nextEvent(500)
                for msg in ev:
                    if cid in msg.correlationIds():
                        securityData = msg.getElement(SECURITY_DATA)
                        ticker = securityData.getElementAsString(SECURITY)
                        fieldDataArray = securityData.getElement(FIELD_DATA)
                        for fieldData in fieldDataArray.values():
                            try:
                                ###########################################################
                                # get date field first
                                ###########################################################
                                fieldValue = fieldData.getElementValue('date')
                                if isinstance(fieldValue, dt.date):
                                    dateField = fieldValue
                                elif isinstance(fieldValue, dt.time):
                                    dateField = dt.datetime.combine(dt.datetime.now().date(), fieldValue)
                                if dateField:
                                    for field in fields:
                                        if fieldData.hasElement(field, True):
                                            fieldValue = fieldData.getElementValue(field)
                                            ###########################################################
                                            # transpose data to secId, fieldId, value, date
                                            ###########################################################
                                            rowData = []
                                            rowData.append(dateField)
                                            rowData.append(ticker)
                                            rowData.append(field)
                                            if isinstance(fieldValue, dt.date):
                                                fieldValue = calendar.timegm(fieldValue.timetuple())
                                            rowData.append(fieldValue)
                                            mktDataAr.append(rowData)
                                else:
                                    logger.error("Error: missing date field %s: %s: %r" % (ticker, field))
                            except Exception as ex:
                                logger.exception("Error Reading data for %s: %r" % (ticker, ex))
                if ev.eventType() == blpapi.Event.RESPONSE:
                    break
            except Exception as ex:
                logger.exception("Error Reading Bloomberg event data: %r" % ex)
    except Exception as ex:
        logger.exception("Error Reading Bloomberg data: %r" % ex)
    else:
        return mktDataAr

def writeMarketData(mktDataAr, output, checkpoint):
    try:
        if len(mktDataAr):
            if output.endswith('.csv'):
                outdir = os.path.dirname(output)
                filename = os.path.basename(output)
            else:
                outdir = output
                filename = 'marketdata.csv'
            columns = ['Date', 'Ticker', 'FieldId', 'Value']
            df = pd.DataFrame(mktDataAr, columns=columns)
            if checkpoint:
                startTicker = mktDataAr[1][1]
                endTicker = mktDataAr[-1][1]
                filename = '%s-%s-%s' % (startTicker, endTicker, filename)
            if not os.path.exists(outdir):
                os.makedirs(outdir, 0o755)
            df.to_csv('%s/%s' % (outdir, filename), header=header)
            logger.info('wrote file: %s' % filename)
    except Exception as ex:
        logger.error('Error saving file: %s\n%r' % (filename, ex))