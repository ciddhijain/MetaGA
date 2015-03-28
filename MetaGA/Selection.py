__author__ = 'Ciddhi'

from Performance import *
import GlobalVariables as gv

class Selection:

    # Select from generation i and insert in generation i+1
    def select(self, generation, dbObject):
        resultPortfolios = dbObject.getPortfolios(generation)
        performanceObject = Performance()
        for portfolioId, size in resultPortfolios:
            performance = performanceObject.calculatePerformance(gv.startDate, gv.endDate, portfolioId, dbObject)
            dbObject.insertPerformance(portfolioId, performance[0][1])
        resultOrdered = dbObject.getOrderedPortfolios(generation)
        for portfolioId, dummy in resultOrdered:
            dbObject.insertUpdateSelectedPortfolio(portfolioId, generation)