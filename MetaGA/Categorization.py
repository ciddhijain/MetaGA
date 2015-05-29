__author__ = 'Ciddhi'

from DBUtils import *
from Performance import *

class Categorization:

    def categorizeFeederIndividuals(self, startDate, endDate, performanceObject, dbObject):
        walkforward = gv.newWalkforward
        resultIndividuals = dbObject.getFeederIndividuals()
        for feederIndividualId, stockId in resultIndividuals:
            performance = performanceObject.calculateReferencePerformanceIndividual(feederIndividualId, stockId, startDate, endDate, dbObject)
            dbObject.updateFeederIndividualPerformance(feederIndividualId, stockId, performance[0][1], walkforward)
            #dbObject.updateFeederIndividualCategory(feederIndividualId, stockId, performance[0][1], walkforward)
        dbObject.updateCategory(walkforward)


if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()
    categoryObject = Categorization()
    performanceObject = Performance()
    categoryObject.categorizeFeederIndividuals(gv.startDate, gv.endDate, performanceObject, dbObject)
    dbObject.dbClose()
