__author__ = 'Ciddhi'

from datetime import timedelta, datetime
import GlobalVariables as gv
import logging

import calendar

class QLearningWrapper:

    def feedback(self, startDate, endDate, portfolioId, performanceObject, rankingObject, mtmObject, rewardMatrixObject, qMatrixObject, trainingObject, liveObject, reallocationObject, dbObject):

        walkforwardStartDate = startDate
        walkforwardEndDate = datetime(walkforwardStartDate.year, walkforwardStartDate.month, calendar.monthrange(walkforwardStartDate.year, walkforwardStartDate.month)[1]).date()
        trainingStartDate = walkforwardEndDate + timedelta(days=1)
        trainingEndDate = datetime(trainingStartDate.year, trainingStartDate.month, calendar.monthrange(trainingStartDate.year, trainingStartDate.month)[1]).date()
        liveStartDate = trainingEndDate + timedelta(days=1)
        liveEndDate = datetime(liveStartDate.year, liveStartDate.month, calendar.monthrange(liveStartDate.year, liveStartDate.month)[1]).date()
        periodEndDate = endDate
        startTime = timedelta(hours=9, minutes=15)

        dbObject.initializeRanks(portfolioId)
        dbObject.resetAssetAllocation(portfolioId, liveStartDate, startTime)
        done = False

        logging.info('Starting q learning for ' + str(portfolioId) + ' from ' + str(walkforwardStartDate) + ' to ' + str(periodEndDate))
        while (not done):
            dbObject.resetAssetTraining(portfolioId)
            rankingObject.updateRankings(portfolioId, walkforwardStartDate, walkforwardEndDate, performanceObject, dbObject)
            trainingObject.train(portfolioId, trainingStartDate, trainingEndDate, mtmObject, rewardMatrixObject, qMatrixObject, dbObject)
            dbObject.resetLatestIndividualsWalkForward(portfolioId)
            liveObject.live(portfolioId, liveStartDate, liveEndDate, dbObject, mtmObject, rewardMatrixObject, qMatrixObject, reallocationObject)
            if liveEndDate>=periodEndDate:
                done = True
            else:
                dbObject.updateQMatrixTableWalkForward(portfolioId)
                dbObject.updateAssetWalkForward(portfolioId)
                dbObject.resetRanks(portfolioId)
                walkforwardStartDate = trainingStartDate
                walkforwardEndDate = trainingEndDate
                trainingStartDate = liveStartDate
                trainingEndDate = liveEndDate
                liveStartDate = trainingEndDate + timedelta(days=1)
                liveEndDate = datetime(liveStartDate.year, liveStartDate.month, calendar.monthrange(liveStartDate.year, liveStartDate.month)[1]).date()
                if liveEndDate>periodEndDate:
                    liveEndDate = periodEndDate
        logging.info('Finished q learning for ' + str(portfolioId))
