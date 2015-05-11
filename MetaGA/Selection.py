__author__ = 'Ciddhi'

from Performance import *
import GlobalVariables as gv
import logging

class Selection:

    # Select from generation i and insert in generation i+1
    def select(self, generation, performanceObject, crossoverObject, dbObject):
        resultPortfolios = dbObject.getPortfolios(generation)
        portfolioList = []
        for portfolioId, size in resultPortfolios:
            portfolioList.append(portfolioId)
        count = 0
        while count<len(portfolioList):
            try:
                resultExisting = dbObject.checkPerformance(portfolioList[count])
                for check, dummy in resultExisting:
                    if check == 0:
                        performance = performanceObject.calculatePerformancePortfolio(gv.startDate, gv.endDate, portfolioList[count], dbObject)
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

        done = False
        while (not done):
            resultCount = dbObject.getOrderedFeasiblePortfolioCount(generation)
            for countPortfolios, dummy in resultCount:
                if countPortfolios>=gv.minNumPortfolios:
                    resultOrdered = dbObject.getOrderedFeasiblePortfolios(generation)
                    for portfolioId, portfolioPerformance in resultOrdered:
                        dbObject.updateSelectedPortfolio(portfolioId)
                    done = True
                else:
                    crossoverObject.performCrossoverRouletteWheel(generation, dbObject, gv.crossoverList)