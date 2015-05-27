__author__ = 'Ciddhi'

from DBUtils import *
from datetime import timedelta

class MTM:

    def calculateMTM (self, portfolioId, individualId, stockId, aggregationUnit, startDate, startTime, endDate, endTime, dbObject):
        # Query to get live trades for the individual
        resultTrades = dbObject.getTradesIndividual(portfolioId, individualId, stockId, startDate, startTime, endDate, endTime)
        for stockId, individualId, entryDate, entryTime, entryPrice, exitDate, exitTime, exitPrice, entryQty, tradeType in resultTrades:
            resultPriceSeries = None
            price = None
            endPrice = None
            resultEndPrice = None
            if entryTime<startTime:
                # To get last price from series to calculate mtm
                resultPrice = dbObject.getPrice(stockId, startDate, startTime)
                for time, openPrice in resultPrice:
                    price = openPrice
                if exitTime>=endTime:
                    resultEndPrice = dbObject.getPrice(stockId, endDate, endTime)
                else:
                    resultEndPrice = dbObject.getPrice(stockId, endDate, exitTime)
                for time, p in resultEndPrice:
                    endPrice = p
            else:
                price = entryPrice
                if exitTime>=endTime:
                    resultEndPrice = dbObject.getPrice(stockId, endDate, endTime)
                else:
                    resultEndPrice = dbObject.getPrice(stockId, endDate, exitTime)
                for time, p in resultEndPrice:
                    endPrice = p
            # mtm calculation
            if tradeType==0:
                if price and endPrice:
                    mtm = (endPrice-price) * entryQty
                    dbObject.addOrUpdateMTM(portfolioId, individualId, stockId, tradeType, entryDate, endTime, mtm)
            else:
                if price and endPrice:
                    mtm = (price-endPrice) * entryQty
                    dbObject.addOrUpdateMTM(portfolioId, individualId, stockId, tradeType, entryDate, endTime, mtm)

    def calculateTrainingMTM (self, portfolioId, individualId, stockId, aggregationUnit, startDate, startTime, endDate, endTime, dbObject):
        # Query to get live trades for the individual
        resultTrades = dbObject.getTrainingTradesIndividual(portfolioId, individualId, stockId, startDate, startTime, endDate, endTime)
        for stockId, individualId, entryDate, entryTime, entryPrice, exitDate, exitTime, exitPrice, entryQty, tradeType in resultTrades:
            resultPriceSeries = None
            price = None
            endPrice = None
            resultEndPrice = None
            if entryTime<startTime:
                # To get last price from series to calculate mtm
                resultPrice = dbObject.getPrice(stockId, startDate, startTime)
                for time, openPrice in resultPrice:
                    price = openPrice
                if exitTime>=endTime:
                    resultEndPrice = dbObject.getPrice(stockId, endDate, endTime)
                else:
                    resultEndPrice = dbObject.getPrice(stockId, endDate, exitTime)
                for time, p in resultEndPrice:
                    endPrice = p
            else:
                price = entryPrice
                if exitTime>=endTime:
                    resultEndPrice = dbObject.getPrice(stockId, endDate, endTime)
                else:
                    resultEndPrice = dbObject.getPrice(stockId, endDate, exitTime)
                for time, p in resultEndPrice:
                    endPrice = p
            # mtm calculation
            if tradeType==0:
                if price and endPrice:
                    mtm = (endPrice-price) * entryQty
                    dbObject.addOrUpdateTrainingMTM(portfolioId, individualId, stockId, tradeType, entryDate, endTime, mtm)
            else:
                if price and endPrice:
                    mtm = (price-endPrice) * entryQty
                    dbObject.addOrUpdateTrainingMTM(portfolioId, individualId, stockId, tradeType, entryDate, endTime, mtm)


if __name__ == "__main__":
    mtmObject = MTM()
    aggregationUnit = 1
    startDate = 20120409
    startTime = timedelta(hours=10, minutes=30)
    endDate = 20120409
    endTime = timedelta(hours=11, minutes=30)
    dbObject = DBUtils()
    dbObject.dbConnect()

    # Reset mtm_table
    #queryReset = "DELETE FROM mtm_table"
    #dbObject.dbQuery(queryReset)
    #mtmObject.calculateMTM(2, aggregationUnit)
    mtmObject.calculateMTM(aggregationUnit, startDate, startTime, endDate, endTime, dbObject)

    #dbObject.insertMTM(1, 11296331, 0, startDate, startTime, 0)
    dbObject.dbClose()