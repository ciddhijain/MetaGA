__author__ = 'Ciddhi'

from DBUtils import *
from Performance import *

class Categorization:

    # This function updates category by considering top x% of the population as elites
    def categorizeFeederIndividualsByPercentage(self, startDate, endDate, performanceObject, dbObject):
        walkforward = gv.newWalkforward
        resultIndividuals = dbObject.getFeederIndividuals()
        for feederIndividualId, stockId in resultIndividuals:
            performance = performanceObject.calculateReferencePerformanceIndividual(feederIndividualId, stockId, startDate, endDate, dbObject)
            dbObject.updateFeederIndividualPerformance(feederIndividualId, stockId, performance[0][1], walkforward)
            #dbObject.updateFeederIndividualCategory(feederIndividualId, stockId, performance[0][1], walkforward)
        dbObject.updateCategory(walkforward)

    # This function updates category by using predefined thresholds
    def categorizeFeederIndividualsByThresholds(self, startDate, endDate, performanceObject, dbObject):
        resultWalkForward = dbObject.getNewWalkforward()
        walkforward = 0
        for wf, dummy in resultWalkForward:
            walkforward = wf
        walkforward += 1
        dbObject.insertWalkForward(walkforward, startDate, endDate)
        resultIndividuals = dbObject.getFeederIndividuals()
        for feederIndividualId, stockId in resultIndividuals:
            performance = performanceObject.calculateReferencePerformanceIndividual(feederIndividualId, stockId, startDate, endDate, dbObject)
            ppt = performance[0][0]
            pl_dd = performance[0][1]
            trades = performance[0][5]
            category = 0
            if trades<4*gv.numTrainingDays or trades>15*gv.numTrainingDays:
                category = 3
            else:
                if pl_dd>5 and ppt>850:
                    category = 1
                elif pl_dd>1/0.3 and ppt>500:
                    category = 2
                else:
                    category = 3
            dbObject.updateFeederIndividualCategory(feederIndividualId, stockId, category, walkforward)


if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()
    categoryObject = Categorization()
    performanceObject = Performance()
    categoryObject.categorizeFeederIndividualsByThresholds(gv.startDate, gv.endDate, performanceObject, dbObject)
    dbObject.dbClose()
