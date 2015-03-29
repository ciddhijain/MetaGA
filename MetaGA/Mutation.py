__author__ = 'Ciddhi'

import GlobalVariables as gv
from random import randint, sample

class Mutation:

    def performMutation(self, generation, dbObject):
        numBits = str(gv.mutationProbability)[::-1].find('.')
        range = 10**numBits
        countNonFeasible = 0
        resultCount = dbObject.getNonFeasibleCount(gv.walkforward)
        for count, dummy in resultCount:
            if count:
                countNonFeasible = count
        resultPortfolios = dbObject.getPortfolios(generation)
        for portfolioId, size in resultPortfolios:
            p = randint(1, range)
            if p<gv.mutationProbability*range:
                oldIndividualId = None
                newIndividualId = None
                resultOldIndividual = dbObject.getRandomPortfolioIndividual(portfolioId, randint(0, size-1))
                for id, dummy in resultOldIndividual:
                    if id:
                        oldIndividualId = id
                resultNewIndividual = dbObject.getRandomNonFeasibleIndividual(randint(0, countNonFeasible-1), gv.walkforward)
                for id, dummy in resultNewIndividual:
                    if id:
                        newIndividualId = id
                if newIndividualId and oldIndividualId:
                    dbObject.insertMutationPortfolio(portfolioId, oldIndividualId, newIndividualId)