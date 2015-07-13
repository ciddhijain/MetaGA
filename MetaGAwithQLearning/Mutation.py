__author__ = 'Ciddhi'

import GlobalVariables as gv
from random import randint, sample

class Mutation:

    def performMutation(self, generation, qLearningObject, performanceObject, rankingObject, mtmObject, rewardMatrixObject, qMatrixObject, trainingObject, liveObject, reallocationObject, dbObject):
        numBits = str(gv.mutationProbability)[::-1].find('.')
        range = 10**numBits

        resultPortfolios = dbObject.getPortfolios(generation)
        for portfolioId, size in resultPortfolios:
            p = randint(1, range)
            if p<gv.mutationProbability*range:
                oldIndividualId = None
                newIndividualId = None
                oldStockId = None
                newStockId = None
                resultOldIndividual = dbObject.getRandomPortfolioIndividual(portfolioId, randint(0, size-1))
                for id, stock in resultOldIndividual:
                    if id:
                        oldIndividualId = id
                        oldStockId = stock
                countNonFeasible = 0
                resultCount = dbObject.getNonFeasibleCount(gv.walkforward)
                for count, dummy in resultCount:
                    if count:
                        countNonFeasible = count
                resultNewIndividual = dbObject.getRandomNonFeasibleIndividual(randint(0, countNonFeasible-1), gv.walkforward)
                for id, stock in resultNewIndividual:
                    if id:
                        newIndividualId = id
                        newStockId = stock
                if newIndividualId and oldIndividualId:
                    newId = dbObject.insertMutationPortfolio(portfolioId, oldIndividualId, newIndividualId, oldStockId, newStockId, generation)
                    qLearningObject.feedback(gv.startDate, gv.endDate, newId, performanceObject, rankingObject, mtmObject, rewardMatrixObject, qMatrixObject, trainingObject, liveObject, reallocationObject, dbObject)
                    performance = performanceObject.calculatePerformancePortfolio(gv.startDate, gv.endDate, newId, dbObject)
                    dbObject.insertPerformance(newId, performance[0][1])
                    dbObject.updatePerformanceFeasibilityPortfolio(newId)