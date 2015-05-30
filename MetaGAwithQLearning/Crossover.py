__author__ = 'Ciddhi'

from random import sample, randint
import GlobalVariables as gv
import logging
import math

class Crossover:

    # Bias type 1 - long long
    # Bias type 3 - short short
    # Bias type 21 - long short
    # Bias type 22 - short long
    def performCrossoverRouletteWheelBiased(self, generation, qLearningObject, performanceObject, rankingObject, mtmObject, rewardMatrixObject, qMatrixObject, trainingObject, liveObject, reallocationObject, dbObject):
        numBits = str(gv.longLongProbability)[::-1].find('.')
        rangeLongLong = 10**numBits
        numBits = str(gv.longShortProbability)[::-1].find('.')
        rangeLongShort = 10**numBits
        numBits = str(gv.shortShortProbability)[::-1].find('.')
        rangeShortShort = 10**numBits
        rangeBias = max(rangeLongLong, rangeLongShort, rangeShortShort)
        numBits = str(gv.crossoverProbability)[::-1].find('.')
        range = 10**numBits
        numPortfolios = math.ceil(gv.numCrossoverPortfolios)
        countFeasiblePortfolios = 0
        resultPortfolios = dbObject.getOrderedPortfolios(generation)
        portfolios = []

        for portfolioId, portfolioPerformance in resultPortfolios:
            portfolios.append((portfolioId, portfolioPerformance))

        # calculating total performance of all portfolios in a generation
        resultTotalPerformance = dbObject.getTotalPerformance(generation)
        totalPerformance = 0
        for total, dummy in resultTotalPerformance:
            if total:
                totalPerformance = total

        # Performing crossover till a fraction of parents are generated
        while (countFeasiblePortfolios<numPortfolios):

            # Finding first portfolio by roulette wheel method
            n = randint(1, math.floor(totalPerformance))
            newIdList = []
            subTotal = 0
            id1 = -1
            id2 = None
            for pair in portfolios:
                subTotal += pair[1]
                if subTotal>=n:
                    id1 = pair[0]
                    break
            resultSize1 = dbObject.getPortfolioSize(id1)

            # We find a random individual till we find one distinct from first one.
            while (True):
                resultSize2 = dbObject.getPortfolioSizeByOffset(randint(0, len(portfolios)-1), generation)
                for size2, id in resultSize2:
                    if id!=id1:
                        cont = False

                        # Ensuring that the pair has already not been in a crossover ????
                        resultCheck = dbObject.checkCrossoverPairs(id, id1, generation)
                        for check, dummy in resultCheck:
                            if check==0:
                                cont = True
                                dbObject.insertCrossoverPair(id, id1, generation)

                        if cont:
                            id2 = id

                            # BiasType 1 corresponds to long-long, 21 corresponds to long-short, 22 corresponds to short-long, 3 corresponds to short-short
                            for size1, dummy in resultSize1:

                                resultLong1 = dbObject.getLongIndividualsPortfolio(id1)
                                resultLong2 = dbObject.getLongIndividualsPortfolio(id2)
                                long1 = None
                                long2 = None
                                short1 = None
                                short2 = None
                                for numLong, dummy in resultLong1:
                                    long1 = numLong
                                    short1 = size1 - long1
                                for numLong, dummy in resultLong2:
                                    long2 = numLong
                                    short2 = size2 - long2

                                k = randint(1, rangeBias)
                                biasType = None

                                # If first individual has no long, either short-short or short-long crossovers are possible
                                if long1==0:
                                    # If second one has no long, only short-short is possible
                                    if long2==0:
                                        biasType = 3
                                    else:
                                        # If second one has no short, only short-long is possible
                                        if short2==0:
                                            biasType = 22
                                        else:
                                            if k<=gv.longShortProbability * rangeBias:
                                                biasType = 22
                                            else:
                                                biasType = 3

                                # If secong individual has no long (and we already know that first one definitely has long),
                                # either short-short or long-short crossovers are possible
                                elif long2==0:
                                    # If first one has no short, only long-short is possible
                                    if short1==0:
                                        biasType = 21
                                    else:
                                        if k<=gv.longShortProbability* rangeBias:
                                            biasType = 21
                                        else:
                                            biasType = 3

                                # If first portfolio has no short (we know that both portfolios definitely have longs),
                                # either long-long or long-short is possible
                                elif short1==0:
                                    # If none has short, only long-long possible
                                    if short2==0:
                                        biasType = 1
                                    else:
                                        if k<(gv.longLongProbability + gv.shortShortProbability) * rangeBias:
                                            biasType = 1
                                        else:
                                            biasType = 21

                                # If second portfolio has no short (we know that first has both long and short),
                                # either long-long or short-long is possible
                                elif short2==0:
                                    if k<(gv.longLongProbability + gv.shortShortProbability) * rangeBias:
                                        biasType = 1
                                    else:
                                        biasType = 21

                                # If both type of individuals exist in both portfolios, all 3 types of crossovers are possible
                                else:
                                    if k<=gv.longLongProbability * rangeBias:
                                        biasType = 1
                                    elif k<=(gv.longShortProbability + gv.longLongProbability) * rangeBias:
                                        if randint(1,100)<=50:
                                            biasType =  21
                                        else:
                                            biasType = 22
                                    else:
                                        biasType = 3


                                if biasType==1:
                                    while(True):
                                        cut1 = randint(1, long1)
                                        cut2 = randint(1, long2)
                                        newSize1 = size1 - cut1 + cut2
                                        newSize2 = size2 - cut2 + cut1
                                        if (newSize1<=gv.maxPortfolioSize and newSize1>=gv.minPortfolioSize and newSize2<=gv.maxPortfolioSize and newSize2>=gv.minPortfolioSize):
                                            newIds = dbObject.insertBiasedCrossoverPortfolio(id1, id2, cut1, cut2, long1, size1, long2, size2, biasType, generation)
                                            for newId in newIds:
                                                newIdList.append(newId)
                                            break

                                elif biasType==21:
                                    while(True):
                                        cut1 = randint(1, long1)
                                        cut2 = randint(1, short2)
                                        newSize1 = size1 - cut1 + cut2
                                        newSize2 = size2 - cut2 + cut1
                                        if (newSize1<=gv.maxPortfolioSize and newSize1>=gv.minPortfolioSize and newSize2<=gv.maxPortfolioSize and newSize2>=gv.minPortfolioSize):
                                            newIds = dbObject.insertBiasedCrossoverPortfolio(id1, id2, cut1, cut2, long1, size1, long2, size2, biasType, generation)
                                            for newId in newIds:
                                                newIdList.append(newId)
                                            break

                                elif biasType==22:
                                    while(True):
                                        cut1 = randint(1, short1)
                                        cut2 = randint(1, long2)
                                        biasType = 22
                                        newSize1 = size1 - cut1 + cut2
                                        newSize2 = size2 - cut2 + cut1
                                        if (newSize1<=gv.maxPortfolioSize and newSize1>=gv.minPortfolioSize and newSize2<=gv.maxPortfolioSize and newSize2>=gv.minPortfolioSize):
                                            newIds = dbObject.insertBiasedCrossoverPortfolio(id1, id2, cut1, cut2, long1, size1, long2, size2, biasType, generation)
                                            for newId in newIds:
                                                newIdList.append(newId)
                                            break

                                else:
                                    while(True):
                                        cut1 = randint(1, short1)
                                        cut2 = randint(1, short2)
                                        newSize1 = size1 - cut1 + cut2
                                        newSize2 = size2 - cut2 + cut1
                                        if (newSize1<=gv.maxPortfolioSize and newSize1>=gv.minPortfolioSize and newSize2<=gv.maxPortfolioSize and newSize2>=gv.minPortfolioSize):
                                            newIds = dbObject.insertBiasedCrossoverPortfolio(id1, id2, cut1, cut2, long1, size1, long2, size2, biasType, generation)
                                            for newId in newIds:
                                                newIdList.append(newId)
                                            break
                            break
                if id2:
                    break
            for newId in newIdList:
                qLearningObject.feedback(newId, performanceObject, rankingObject, mtmObject, rewardMatrixObject, qMatrixObject, trainingObject, liveObject, reallocationObject, dbObject)
                performance = performanceObject.calculatePerformancePortfolio(gv.startDate, gv.endDate, newId, dbObject)
                dbObject.insertPerformance(newId, performance[0][1])
                feasiblePerformance = dbObject.updatePerformanceFeasibilityPortfolio(newId)
                if feasiblePerformance:
                    countFeasiblePortfolios += 1
        return None

    # Function to perform Crossover.
    # By default, it performs single point crossover (type=1).
    # Two point crossover corresponds to 'type=2'.
    # Uniform crossover corresponds to 'type=3'.
    # The variable 'type' takes a list of lists [(crossoverType, numChildren)] as input and
    # performs all listed types of crossovers.
    # By default, a single point crossover gives two children.
    # The variable numChildren can take a list of values (each belonging to {1, 2}),
    # and gives corresponding number of children for respective type of crossover.

    def performCrossoverRouletteWheel(self, generation, dbObject, type=[(1,2)]):
        numBits = str(gv.crossoverProbability)[::-1].find('.')
        range = 10**numBits
        numParents = math.ceil(gv.numPortfolios/2)
        count = 0
        resultPortfolios = dbObject.getOrderedPortfolios(generation)
        portfolios = []

        for portfolioId, portfolioPerformance in resultPortfolios:
            portfolios.append((portfolioId, portfolioPerformance))

        # calculating total performance of all portfolios in a generation
        resultTotalPerformance = dbObject.getTotalPerformance(generation)
        totalPerformance = 0
        for total, dummy in resultTotalPerformance:
            if total:
                totalPerformance = total

        # Performing crossover till a fraction of parents are generated
        while (count<numParents):

            p = randint(1, range)
            if p<gv.crossoverProbability*range:

                # Finding first portfolio by roulette wheel method
                n = randint(1, math.ceil(totalPerformance))
                subTotal = 0
                id1 = -1
                id2 = None
                for pair in portfolios:
                    subTotal += pair[1]
                    if subTotal>=n:
                        id1 = pair[0]
                        break
                resultSize1 = dbObject.getPortfolioSize(id1)

                # We find a random individual till we find one distinct from first one.
                while (True):
                    resultSize2 = dbObject.getPortfolioSizeByOffset(randint(0, len(portfolios)-1), generation)
                    for size2, id in resultSize2:
                        if id!=id1:
                            cont = False

                            # Ensuring that the pair has already not been in a crossover ????
                            resultCheck = dbObject.checkCrossoverPairs(id, id1, generation)
                            for check, dummy in resultCheck:
                                if check==1:
                                    cont = True
                                    dbObject.insertCrossoverPair(id, id1, generation)

                            if cont:
                                id2 = id
                                for size1, dummy in resultSize1:
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
                                break
                    if id2:
                        break
            count += 1
        return None

    # NOTE - This function has not been adjusted corresponding to change in DB structure
    def performCrossover(self, generation, dbObject, type=[(1, 2)]):

        groups = sample(range(gv.numPortfolios), gv.numPortfolios)          # This provides a random ordered offset for pairing
        logging.info("Combinations generated for crossover : ")

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