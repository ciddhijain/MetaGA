__author__ = 'Ciddhi'

from datetime import timedelta, datetime
import GlobalVariables as gv

import calendar

class QLearningWrapper:

    def feedback(self, portfolioId, performanceObject, rankingObject, mtmObject, rewardMatrixObject, qMatrixObject, trainingObject, liveObject, reallocationObject, dbObject):
        '''
        walkforwardStartDate = gv.startDate
        walkforwardEndDate = datetime(walkforwardStartDate.year, walkforwardStartDate.month, calendar.monthrange(walkforwardStartDate.year, walkforwardStartDate.month)[1]).date()
        trainingStartDate = walkforwardEndDate + timedelta(days=1)
        trainingEndDate = datetime(trainingStartDate.year, trainingStartDate.month, calendar.monthrange(trainingStartDate.year, trainingStartDate.month)[1]).date()
        liveStartDate = trainingEndDate + timedelta(days=1)
        liveEndDate = datetime(liveStartDate.year, liveStartDate.month, calendar.monthrange(liveStartDate.year, liveStartDate.month)[1]).date()
        periodEndDate = gv.endDate
        startTime = timedelta(hours=9, minutes=15)
        '''
        walkforwardStartDate = gv.startDate
        walkforwardEndDate = walkforwardStartDate + timedelta(days=1)
        trainingStartDate = walkforwardEndDate + timedelta(days=1)
        trainingEndDate = trainingStartDate + timedelta(days=1)
        liveStartDate = trainingEndDate + timedelta(days=1)
        liveEndDate = liveStartDate + timedelta(days=1)
        periodEndDate = walkforwardStartDate + timedelta(days=12)
        startTime = timedelta(hours=9, minutes=15)

        dbObject.initializeRanks(portfolioId)
        dbObject.resetAssetAllocation(portfolioId, liveStartDate, startTime)
        done = False

        print('Started at : ' + str(datetime.now()))
        while (not done):
            dbObject.resetLatestIndividualsWalkForward(portfolioId)
            dbObject.resetAssetTraining(portfolioId)
            rankingObject.updateRankings(portfolioId, walkforwardStartDate, walkforwardEndDate, performanceObject, dbObject)
            trainingObject.train(portfolioId, trainingStartDate, trainingEndDate, mtmObject, rewardMatrixObject, qMatrixObject, dbObject)
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
        print('Finished at : ' + str(datetime.now()))
