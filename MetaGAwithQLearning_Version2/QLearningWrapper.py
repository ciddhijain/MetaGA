__author__ = 'Ciddhi'

from datetime import timedelta, datetime
import GlobalVariables as gv
import logging

import calendar

class QLearningWrapper:

    def feedback(self, startDate, endDate, portfolioId, performanceObject, rankingObject, mtmObject, rewardMatrixObject, qMatrixObject, trainingObject, liveObject, reallocationObject, dbObject):

        rankingStartDate = startDate
        rankingEndDate = rankingStartDate + timedelta(days=gv.rankingDays)
        trainingStartDate = rankingEndDate + timedelta(days=1)
        trainingEndDate = trainingStartDate + timedelta(days=gv.initializationDays)
        liveStartDate = trainingEndDate + timedelta(days=1)
        liveEndDate = liveStartDate + timedelta(days=gv.liveDays)
        periodEndDate = endDate
        startTime = timedelta(hours=9, minutes=15)

        dbObject.initializeRanks(portfolioId)
        dbObject.resetAssetAllocation(portfolioId, liveStartDate, startTime)
        done = False

        logging.info('Starting q learning for ' + str(portfolioId) + ' from ' + str(rankingStartDate) + ' to ' + str(periodEndDate))
        while (not done):
            dbObject.resetAssetTraining(portfolioId)
            rankingObject.updateRankings(portfolioId, rankingStartDate, rankingEndDate, performanceObject, dbObject)
            trainingObject.train(portfolioId, trainingStartDate, trainingEndDate, mtmObject, rewardMatrixObject, qMatrixObject, dbObject)
            dbObject.resetLatestIndividualsWalkForward(portfolioId)
            liveObject.live(portfolioId, liveStartDate, liveEndDate, dbObject, mtmObject, rewardMatrixObject, qMatrixObject, reallocationObject)
            if liveEndDate>=periodEndDate:
                done = True
            else:
                dbObject.updateQMatrixTableWalkForward(portfolioId)
                dbObject.updateAssetWalkForward(portfolioId)
                dbObject.resetRanks(portfolioId)
                trainingEndDate = liveEndDate
                trainingStartDate = trainingEndDate - timedelta(days=gv.initializationDays)
                rankingEndDate = trainingStartDate - timedelta(days=1)
                rankingStartDate = rankingEndDate - timedelta(days=gv.rankingDays)
                liveStartDate = liveEndDate + timedelta(days=1)
                liveEndDate = liveStartDate + timedelta(days=gv.liveDays)
                if liveEndDate>periodEndDate:
                    liveEndDate = periodEndDate
        logging.info('Finished q learning for ' + str(portfolioId))
