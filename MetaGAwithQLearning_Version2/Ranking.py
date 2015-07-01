__author__ = 'Ciddhi'

import GlobalVariables as gv
import datetime
from DBUtils import *

class Ranking:

    def updateRankings(self, portfolioId, startDate, endDate, performanceObject, dbObject):
        resultIndividuals = dbObject.getPortfolioIndividuals(portfolioId)
        individualperformanceList = []

        # fetching performance for all individuals
        for individualId, stockId in resultIndividuals:
            performance = performanceObject.calculateReferencePerformanceIndividual(individualId, stockId, startDate, endDate, dbObject)
            individualperformanceList.append((individualId, stockId, performance[0][1]))

        # Sorting the individuals according to performance
        individualperformanceList.sort(key=lambda tup: -tup[2])

        # Updating ranks in db
        for i in range(0, len(individualperformanceList), 1):
            if individualperformanceList[i][2] != gv.dummyPerformance:
                dbObject.updateRank(portfolioId, individualperformanceList[i][0], individualperformanceList[i][1], i+1)

if __name__ == "__main__":
    rankingObject = Ranking()
    dbObject = DBUtils()
    dbObject.dbConnect()
    date = datetime(2012, 1, 2).date()
    periodEndDate = datetime(2012, 1, 10).date()
    rankingObject.updateRankings(date, periodEndDate, dbObject)
    dbObject.dbClose()