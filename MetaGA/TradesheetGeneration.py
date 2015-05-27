__author__ = 'Ciddhi'

from datetime import timedelta, datetime
import GlobalVariables as gv
import logging

class TradesheetGeneration:

    def generateTradesheet(self, portfolioId, startDate, endDate, dbObject):
        date = startDate
        periodEndDate = endDate
        startTime = timedelta(hours=9, minutes=15)
        dayEndTime = timedelta(hours=15, minutes=30)
        lastCheckedTime = timedelta(hours=9, minutes=15)
        done = False

        logging.info('\n')
        logging.info('Portfolio Id : ' + str(portfolioId))
        logging.info('Starting Tradesheet Generation from ' + str(date) + ' to ' + str(endDate))

        while (not done):
            isTradingDay = False
            resultTrades = dbObject.getDayTrades(portfolioId, date)
            dbObject.setInitialExposure(portfolioId, date, startTime)
            for stockId, individualId, entryDate, entryTime, entryPrice, exitDate, exitTime, exitPrice, entryQty, tradeType in resultTrades:
                if stockId:
                    isTradingDay = True
                    resultStockExposure = dbObject.getCurrentStockExposure(portfolioId, stockId, date, entryTime)

