__author__ = 'Ciddhi'

import GlobalVariables as gv

class Exposure:

    # This function calculates stock-wise exposure for a given portfolio
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