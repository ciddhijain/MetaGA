__author__ = 'Ciddhi'

from datetime import timedelta, datetime
import GlobalVariables as gv
import logging

class TradesheetGeneration:

    def generateTradesheet(self, portfolioId, startDate, endDate, dbObject):
        date = startDate
        periodEndDate = endDate
        startTime = timedelta(hours=9, minutes=15)
        done = False

        logging.info('\n')
        logging.info('Portfolio Id : ' + str(portfolioId))
        logging.info('Starting Tradesheet Generation from ' + str(date) + ' to ' + str(endDate))

        while (not done):
            resultTrades = dbObject.getDayTrades(portfolioId, date)
            dbObject.setInitialExposure(portfolioId, date, startTime)
            for stockId, individualId, entryDate, entryTime, entryPrice, exitDate, exitTime, exitPrice, entryQty, tradeType in resultTrades:
                if stockId:
                    [totalExposure, stockExposure] = dbObject.updateAndGetCurrentExposure(portfolioId, stockId, date, entryTime)
                    if totalExposure<gv.thresholdPortfolioExposure and stockExposure<gv.thresholdStockExposure:
                        dbObject.insertTrade(portfolioId, stockId, individualId, entryDate, entryTime, entryPrice, exitDate, exitTime, exitPrice, entryQty, tradeType)
                        newExposure = 0
                        if tradeType==0:
                            newExposure = entryQty * entryPrice
                        else:
                            newExposure = entryQty * entryPrice * (-1)
                        dbObject.updateNewExposure(portfolioId, individualId, stockId, date, entryTime, newExposure)
            date = date + timedelta(days=1)
            if(date>periodEndDate):
                done = True