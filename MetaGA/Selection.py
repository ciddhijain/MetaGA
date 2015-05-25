__author__ = 'Ciddhi'

from Performance import *
import GlobalVariables as gv
import logging

class Selection:

    # Select from generation i and insert in generation i+1
    def select(self, generation, performanceObject, dbObject):

        resultOrdered = dbObject.getOrderedFeasiblePortfolios(generation)
        for portfolioId, portfolioPerformance in resultOrdered:
            dbObject.updateSelectedPortfolio(portfolioId)