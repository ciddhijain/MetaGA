__author__ = 'Ciddhi'

import GlobalVariables as gv
from random import randint

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

            # This list stores the offsets generated, to ensure the individuals in a portfolio are unique
            # Inherent offset for elites is 0, for feasible individuals is countElite+0 and
            # for non-feasible individuals is countElite+countFeasible+0
            portfolioOffsets = []

            # Terminate when required number of individuals have been added to the portfolio
            while currentSize<sizePortfolio:

                # Generating probability for fetching elite/feasible feeder individual
                p = randint(1, range)

                if p<gv.feederEliteSelectionProbability*range:
                    # Generating a unique offset for elite individuals
                    offset = 0
                    while (True):
                        offset = randint(0, countElite-1)
                        if offset not in portfolioOffsets:
                            portfolioOffsets.append(offset)
                            break
                    resultIndividual = dbObject.getRandomEliteIndividualLatest(offset)
                else:
                    # Generating a unique offset for feasible individuals
                    offset = 0
                    while (True):
                        offset = randint(0, countFeasible-1)
                        if offset not in portfolioOffsets:
                            portfolioOffsets.append(countElite+offset)
                            break
                    resultIndividual = dbObject.getRandomFeasibleIndividualLatest(offset)
                for individualId, dummy in resultIndividual:
                    currentSize += 1
                    dbObject.insertPortfolioMapping(countPortfolios+1, individualId)

            countPortfolios += 1




if __name__ == "__main__":
    combinationObj = CombinationToLists()
    combinationObj.combine()