__author__ = 'Ciddhi'

import GlobalVariables as gv
from DBUtils import *

class Exposure:

    # This function calculates stock-wise exposure for a given portfolio
    # Delete later
    def calculateExposurePortfolioStock(self, portfolioId, stockId, dbObject):
        resultTrades = dbObject.getTradesPortfolioStock(portfolioId, stockId, gv.startDate, gv.endDate)

        for stock, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice in resultTrades:
            # For intra-day trading
            if entryDate==exitDate:
                resultPriceSeries = dbObject.getPriceSeriesDayDuration(stock, entryDate, entryTime, exitTime)
                for date, time, price in resultPriceSeries:
                    dbObject.addOrUpdateStockExposure(portfolioId, stock, entryPrice, entryQty, tradeType, date, time, price)
            # For inter-day trading
            else:
                resultPriceSeriesDayEnd = dbObject.getPriceSeriesDayEnd(stock, entryDate, entryTime)
                for date, time, price in resultPriceSeriesDayEnd:
                    dbObject.addOrUpdateStockExposure(portfolioId, stock, entryPrice, entryQty, tradeType, date, time, price)
                resultPriceSeriesDuration = dbObject.getPriceSeriesDays(stock, entryDate, exitDate)
                for date, time, price in resultPriceSeriesDuration:
                    dbObject.addOrUpdateStockExposure(portfolioId, stock, entryPrice, entryQty, tradeType, date, time, price)
                resultPriceSeriesStart = dbObject.getPriceSeriesDayStart(stock, exitDate, exitTime)
                for date, time, price in resultPriceSeriesStart:
                    dbObject.addOrUpdateStockExposure(portfolioId, stock, entryPrice, entryQty, tradeType, date, time, price)

    # This function calculates exposure for every individual in database
    def calculateExposureIndividuals(self, dbObject):
        resultIndividuals = dbObject.getFeederIndividuals(gv.walkforward)
        for individualId, stockId in resultIndividuals:
            resultTrades = dbObject.getTradesFeederIndividuals(individualId, stockId, gv.startDate, gv.endDate)
            for stock, individualId, tradeType, entryDate, entryTime, entryPrice, entryQty, exitDate, exitTime, exitPrice in resultTrades:
                # For intra-day trading
                if entryDate==exitDate:
                    resultPriceSeries = dbObject.getPriceSeriesDayDuration(stock, entryDate, entryTime, exitTime)
                    for date, time, price in resultPriceSeries:
                        dbObject.addOrUpdateStockExposure(individualId, stock, entryPrice, entryQty, tradeType, date, time, price)
                # For inter-day trading
                else:
                    resultPriceSeriesDayEnd = dbObject.getPriceSeriesDayEnd(stock, entryDate, entryTime)
                    for date, time, price in resultPriceSeriesDayEnd:
                        dbObject.addOrUpdateStockExposure(individualId, stock, entryPrice, entryQty, tradeType, date, time, price)
                    resultPriceSeriesDuration = dbObject.getPriceSeriesDays(stock, entryDate, exitDate)
                    for date, time, price in resultPriceSeriesDuration:
                        dbObject.addOrUpdateStockExposure(individualId, stock, entryPrice, entryQty, tradeType, date, time, price)
                    resultPriceSeriesStart = dbObject.getPriceSeriesDayStart(stock, exitDate, exitTime)
                    for date, time, price in resultPriceSeriesStart:
                        dbObject.addOrUpdateStockExposure(individualId, stock, entryPrice, entryQty, tradeType, date, time, price)

if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()
    exposureObject = Exposure()
    exposureObject.calculateExposureIndividuals(dbObject)
    dbObject.dbClose()
