__author__ = 'Ciddhi'

from random import sample, randint
import GlobalVariables as gv

class Crossover:

    # Function to perform Crossover.
    # By default, it performs single point crossover (type=1).
    # Two point crossover corresponds to 'type=2'.
    # Uniform crossover corresponds to 'type=3'.
    # The variable 'type' takes a list of lists [(crossoverType, numChildren)] as input and
    # performs all listed types of crossovers.
    # By default, a single point crossover gives two children.
    # The variable numChildren can take a list of values (each belonging to {1, 2}),
    # and gives corresponding number of children for respective type of crossover.

    def performCrossover(self, generation, dbObject, type=[(1, 2)]):

        groups = sample(range(gv.numPortfolios), gv.numPortfolios)          # This provides a random ordered offset for pairing

        for i in range(0, gv.numPortfolios, 2):
            if i+1<gv.numPortfolios:

                # Getting size of both portfolios
                resultSize1 = dbObject.getPortfolioSizeByOffset(groups[i], generation)
                resultSize2 = dbObject.getPortfolioSizeByOffset(groups[i+1], generation)
                for size1, id1 in resultSize1:
                    for size2, id2 in resultSize2:

                        # Performing crossover for all provided types
                        for detailedType in type:
                            crossoverType = detailedType[0]
                            numChildren = detailedType[1]

                            # This corresponds to Single Point Crossover
                            if crossoverType==1:
                                while (True):
                                    cut11 = randint(1, size1-1)
                                    cut21 = randint(1, size2-1)
                                    newSize1 = cut11 + size2 - cut21
                                    newSize2 = cut21 + size1 - cut11
                                    if newSize1<=gv.maxPortfolioSize and newSize1>=gv.minPortfolioSize and newSize2<=gv.maxPortfolioSize and newSize2>=gv.minPortfolioSize:
                                        dbObject.insertCrossoverPortfolio(crossoverType, numChildren, id1, cut11, 0, id2, cut21, 0, generation)
                                        break

                            # This corresponds to Two Point Crossover
                            if crossoverType==2:
                                while (True):
                                    cut11 = randint(1, size1-1)
                                    cut12 = randint(cut11, size1-1)
                                    cut21 = randint(1, size2-1)
                                    cut22 = randint(cut21, size2-1)
                                    newSize1 = cut11 + cut22 - cut21 + size1 - cut12
                                    newSize2 = cut21 + cut12 - cut11 + size2 - cut22
                                    if newSize1<=gv.maxPortfolioSize and newSize1>=gv.minPortfolioSize and newSize2<=gv.maxPortfolioSize and newSize2>=gv.minPortfolioSize:
                                        dbObject.insertCrossoverPortfolio(crossoverType, numChildren, id1, cut11, cut12, id2, cut21, cut22, generation)
                                        break

if __name__ == "__main__":
    crossoverObj = Crossover()
    crossoverObj.performCrossover(1)