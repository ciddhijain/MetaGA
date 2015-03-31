__author__ = 'Ciddhi'

from Performance import *
import GlobalVariables as gv
import logging

class Selection:

    # Select from generation i and insert in generation i+1
    def select(self, generation, dbObject):
        resultPortfolios = dbObject.getPortfolios(generation)
        performanceObject = Performance()
        portfolioList = []
        for portfolioId, size in resultPortfolios:
            portfolioList.append(portfolioId)
        count = 0
        while count<len(portfolioList):
            try:
                resultExisting = dbObject.checkPerformance(portfolioList[count])
                for check, dummy in resultExisting:
                    if check == 0:
                        performance = performanceObject.calculatePerformance(gv.startDate, gv.endDate, portfolioList[count], dbObject)
                        dbObject.insertPerformance(portfolioList[count], performance[0][1])
                        count += 1
                    else:
                        count += 1
            except Exception, e:
                logging.error(e)
                #print(e)
                #logging.info("Reconnecting to database ---------")
                #dbObject.dbClose()
                #dbObject.dbConnect()
        resultOrdered = dbObject.getOrderedPortfolios(generation)
        for portfolioId, portfolioPerformance in resultOrdered:
            dbObject.insertUpdateSelectedPortfolio(portfolioId, portfolioPerformance, generation)