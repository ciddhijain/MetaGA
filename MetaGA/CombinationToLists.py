__author__ = 'Ciddhi'

import GlobalVariables as gv
from random import randint, sample

class CombinationToLists:

    def combine(self, dbObject):
        numBitsEliteProbabilty = str(gv.feederEliteSelectionProbability)[::-1].find('.')
        numBitsNonEliteProbability = str(gv.feederNonEliteSelectionProbability)[::-1].find('.')
        numBits = max(numBitsEliteProbabilty, numBitsNonEliteProbability)
        range = 10**numBits
        countPortfolios = 0
        countElite = 0
        countFeasible = 0
        countNonFeasible = 0

        # To get the number of each type of individuals in table
        resultCountElite = dbObject.getEliteCountLatest()
        resultCountFeasible = dbObject.getFeasibleCountLatest()
        resultCountNonFeasible = dbObject.getNonFeasibleCountLatest()
        for count, dummy in resultCountElite:
            if count:
                countElite = count
        for count, dummy in resultCountFeasible:
            if count:
                countFeasible = count
        for count, dummy in resultCountNonFeasible:
            if count:
                countNonFeasible = count

        # Terminate when required number of combinations have been generated
        while countPortfolios<gv.numPortfolios:

            # We generate a list of following size
            sizePortfolio = randint(gv.minPortfolioSize, gv.maxPortfolioSize)
            currentSize = 0

            # Lists containing unique randomly ordered offsets within range for elites and feasible individuals
            eliteOffsets = sample(range(countElite), sizePortfolio)
            feasibleOffsets = sample(range(countFeasible), sizePortfolio)

            # Terminate when required number of individuals have been added to the portfolio
            while currentSize<sizePortfolio:

                # Generating probability for fetching elite/feasible feeder individual
                p = randint(1, range)
                if p<gv.feederEliteSelectionProbability*range:
                    resultIndividual = dbObject.getRandomEliteIndividualLatest(eliteOffsets[currentSize])
                else:
                    resultIndividual = dbObject.getRandomFeasibleIndividualLatest(feasibleOffsets[currentSize])
                for individualId, dummy in resultIndividual:
                    currentSize += 1
                    dbObject.insertPortfolioMapping(countPortfolios+1, individualId, 0, 0)

            countPortfolios += 1


if __name__ == "__main__":
    combinationObj = CombinationToLists()
    combinationObj.combine()