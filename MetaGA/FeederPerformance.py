__author__ = 'Ciddhi'

from Performance import *

if __name__ == "__main__":
    performanceObject = Performance()
    dbObject = DBUtils()
    dbObject.dbConnect()
    resultFeeder = dbObject.getFeederIndividuals(gv.walkforward)
    for individualId, dummy in resultFeeder:
        performance = performanceObject.calculatePerformanceIndividuals(gv.startDate, gv.endDate, individualId, dbObject)
    dbObject.dbClose()
