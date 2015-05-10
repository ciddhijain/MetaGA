__author__ = 'Ciddhi'

import GlobalVariables as gv
from random import randint, sample

class Mutation:

    def performMutation(self, generation, dbObject):
        numBits = str(gv.mutationProbability)[::-1].find('.')
        range = 10**numBits

        resultPortfolios = dbObject.getPortfolios(generation)
        for portfolioId, size in resultPortfolios:
            p = randint(1, range)
            if p<gv.mutationProbability*range:
                oldIndividualId = None
                newIndividualId = None
                stockId = None
                resultOldIndividual = dbObject.getRandomPortfolioIndividual(portfolioId, randint(0, size-1))
                for id, stock in resultOldIndividual:
                    if id:
                        oldIndividualId = id
                        stockId = stock
                countNonFeasible = 0
                resultCount = dbObject.getNonFeasibleCountStock(gv.walkforward, stockId)
                for count, dummy in resultCount:
                    if count:
                        countNonFeasible = count
                resultNewIndividual = dbObject.getRandomNonFeasibleIndividualStock(randint(0, countNonFeasible-1), gv.walkforward, stockId)
                for id, dummy in resultNewIndividual:
                    if id:
                        newIndividualId = id
                if newIndividualId and oldIndividualId:
                    dbObject.insertMutationPortfolio(portfolioId, oldIndividualId, newIndividualId, stockId, generation)