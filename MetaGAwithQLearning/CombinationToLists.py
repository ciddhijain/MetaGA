__author__ = 'Ciddhi'

import GlobalVariables as gv
from random import randint, sample
import logging

class CombinationToLists:

    def combine(self, qLearningObject, performanceObject, rankingObject, mtmObject, rewardMatrixObject, qMatrixObject, trainingObject, liveObject, reallocationObject, dbObject):
        numBitsEliteProbabilty = str(gv.feederEliteSelectionProbability)[::-1].find('.')
        numBitsNonEliteProbability = str(gv.feederNonEliteSelectionProbability)[::-1].find('.')
        numBits = max(numBitsEliteProbabilty, numBitsNonEliteProbability)
        rangeBits = 10**numBits
        countPortfolios = 0
        countFeasiblePortfolios = 0
        countElite = 0
        countFeasible = 0
        countNonFeasible = 0

        # To get the number of each type of individuals in table
        resultCountElite = dbObject.getEliteCount(gv.walkforward)
        resultCountFeasible = dbObject.getFeasibleCount(gv.walkforward)
        resultCountNonFeasible = dbObject.getNonFeasibleCount(gv.walkforward)
        for count, dummy in resultCountElite:
            if count:
                countElite = count
        logging.info("Number of elites - " + str(countElite))
        for count, dummy in resultCountFeasible:
            if count:
                countFeasible = count
        logging.info("Number of Feasible individuals - " + str(countFeasible))
        for count, dummy in resultCountNonFeasible:
            if count:
                countNonFeasible = count
        logging.info("Number of Non Feasible individuals - " + str(countNonFeasible))

        logging.info("Generating Combinations :")
        print("Generating initial combinations")

        # Terminate when required number of combinations have been generated
        while countFeasiblePortfolios<gv.numPortfolios:

            # We generate a list of following size
            sizePortfolio = randint(gv.minPortfolioSize, gv.maxPortfolioSize)
            currentSize = 0

            # Lists containing unique randomly ordered offsets within range for elites and feasible individuals
            eliteOffsets = sample(range(countElite), sizePortfolio)
            feasibleOffsets = sample(range(countFeasible), sizePortfolio)

            # Terminate when required number of individuals have been added to the portfolio
            while currentSize<sizePortfolio:

                # Generating probability for fetching elite/feasible feeder individual
                p = randint(1, rangeBits)
                if p<=gv.feederEliteSelectionProbability*rangeBits:
                    resultIndividual = dbObject.getRandomEliteIndividual(eliteOffsets[currentSize], gv.walkforward)
                else:
                    resultIndividual = dbObject.getRandomFeasibleIndividual(feasibleOffsets[currentSize], gv.walkforward)
                for individualId, stockId in resultIndividual:
                    currentSize += 1
                    dbObject.insertPortfolioMapping(countPortfolios+1, individualId, stockId)

            dbObject.insertPortfolio(countPortfolios+1, 1, 1)

            qLearningObject.feedback(countPortfolios+1, performanceObject, rankingObject, mtmObject, rewardMatrixObject, qMatrixObject, trainingObject, liveObject, reallocationObject, dbObject)
            performance = performanceObject.calculatePerformancePortfolio(gv.startDate, gv.endDate, countPortfolios+1, dbObject)
            dbObject.insertPerformance(countPortfolios+1, performance[0][1])

            feasiblePerformance = dbObject.updatePerformanceFeasibilityPortfolio(countPortfolios+1)
            if feasiblePerformance==1:
                countFeasiblePortfolios += 1
                logging.info("Feasible Portfolio - incremented feasible count to " + str(countFeasiblePortfolios))
            countPortfolios += 1

        logging.info("Generated " + str(countPortfolios) + " total combinations and " + str(countFeasiblePortfolios) + " feasible combinations")


if __name__ == "__main__":
    combinationObj = CombinationToLists()
    combinationObj.combine()