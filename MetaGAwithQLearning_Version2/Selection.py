__author__ = 'Ciddhi'

from Performance import *
import GlobalVariables as gv
import logging

class Selection:

    # Select from generation i and insert in generation i+1
    def select(self, generation, dbObject):
        portfolios = []

        resultOrdered = dbObject.getOrderedFeasiblePortfolios(generation)
        for portfolioId, portfolioPerformance in resultOrdered:
            portfolios.append((portfolioId, portfolioPerformance))
            dbObject.updateSelectedPortfolio(portfolioId)

        maxPerformance = portfolios[len(portfolios)-1][1]
        minPerformance = maxPerformance - gv.admissiblePerformanceGap
        resultRangeOrdered = dbObject.getOrderedFeasiblePortfoliosPerformanceRange(generation, minPerformance, maxPerformance)

        for portfolioId, portfolioPerformance in resultRangeOrdered:
            #portfolios.append((portfolioId, portfolioPerformance))
            dbObject.updateSelectedPortfolio(portfolioId)