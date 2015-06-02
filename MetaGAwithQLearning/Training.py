__author__ = 'Ciddhi'

from datetime import timedelta, datetime
import GlobalVariables as gv
import logging

class Training:

    def train(self, portfolioId, startDate, endDate, mtmObject, rewardMatrixObject, qMatrixObject, dbObject):
        date = startDate
        periodEndDate = endDate
        startTime = timedelta(hours=9, minutes=15)
        endTime = timedelta(hours=10, minutes=30)
        dayEndTime = timedelta(hours=15, minutes=30)
        lastCheckedTime = timedelta(hours=9, minutes=15)
        done = False
        logging.info('\n')
        logging.info('Portfolio Id : ' + str(portfolioId))
        logging.info('Starting Training from ' + str(date) + ' to ' + str(endDate))

        while (not done):
            resultTradingDay = dbObject.checkTradingDay(date)
            for checkTradingDay, dummy0 in resultTradingDay:
                if checkTradingDay==1:
                    resultTrades = dbObject.getRankedTradesOrdered(portfolioId, date, startTime, endTime)
                    for stockId, individualId, entryDate, entryTime, entryPrice, exitDate, exitTime, exitPrice, entryQty, tradeType in resultTrades:
                        if stockId:
                            resultTradesExit = dbObject.getTrainingTradesExit(portfolioId, date, lastCheckedTime, entryTime)
                            for id, stock, type, qty, entry_price, exit_price in resultTradesExit:
                                #print('Exiting Trades')
                                freedAsset = 0
                                if type==1:
                                    freedAsset = qty*exit_price*(-1)
                                else:
                                    freedAsset = qty*(2*entry_price - exit_price)*(-1)
                                dbObject.updateTrainingIndividualAsset(portfolioId, gv.dummyIndividualId, gv.dummyStockId, freedAsset)
                                dbObject.updateTrainingIndividualAsset(portfolioId, id, stock, freedAsset)
                            lastCheckedTime = entryTime
                            resultAvailable = dbObject.getTrainingFreeAsset(portfolioId, gv.dummyIndividualId, gv.dummyStockId)
                            usedAsset = entryQty*entryPrice
                            for freeAssetTotal, dummy1 in resultAvailable:
                                if float(freeAssetTotal)>=usedAsset:
                                    #print('Overall asset is available')
                                    resultExists = dbObject.checkTrainingIndividualAssetExists(portfolioId, individualId, stockId)
                                    for exists, dummy2 in resultExists:
                                        if exists==0:
                                            #print('Individual does not exist in asset table yet. Adding it.')
                                            dbObject.addTrainingIndividualAsset(portfolioId, individualId, stockId, usedAsset)
                                            #print('Taking this trade. Asset used = ' + str(usedAsset))
                                            dbObject.insertTrainingNewTrade(portfolioId, stockId, individualId, entryDate, entryTime, entryPrice, exitDate, exitTime, exitPrice, entryQty, tradeType)
                                            dbObject.updateTrainingIndividualAsset(portfolioId, gv.dummyIndividualId, gv.dummyStockId, usedAsset)
                                        else:
                                            #print('Individual exists already')
                                            resultFreeAsset = dbObject.getTrainingFreeAsset(portfolioId, individualId, stockId)
                                            for freeAsset, dummy3 in resultFreeAsset:
                                                if freeAsset>=usedAsset:
                                                    #print('Individual Asset is available. Taking this trade. Asset used = ' + str(usedAsset))
                                                    dbObject.insertTrainingNewTrade(portfolioId, stockId, individualId, entryDate, entryTime, entryPrice, exitDate, exitTime, exitPrice, entryQty, tradeType)
                                                    dbObject.updateTrainingIndividualAsset(portfolioId, gv.dummyIndividualId, gv.dummyStockId, usedAsset)
                                                    dbObject.updateTrainingIndividualAsset(portfolioId, individualId, stockId, usedAsset)

                    resultIndividuals = dbObject.getTrainingIndividuals(portfolioId, date, startTime, date, endTime)
                    for individualId, stockId in resultIndividuals:
                        resultCheck = dbObject.checkQMatrix(portfolioId, individualId, stockId)
                        for check, dummy4 in resultCheck:
                            if check==0:
                                print('Calculating mtm')
                                mtmObject.calculateTrainingMTM(portfolioId, individualId, stockId, date, startTime, date, endTime, dbObject)
                                print('Calculating reward matrix')
                                rewardMatrix = rewardMatrixObject.computeTrainingRM(portfolioId, individualId, stockId, date, startTime, date, endTime, dbObject)
                                print('Calculating q matrix')
                                qMatrixObject.calculateQMatrix(rewardMatrix, portfolioId, individualId, stockId, dbObject)
                    if endTime<dayEndTime:
                        startTime = endTime
                        endTime = endTime + timedelta(hours=gv.hourWindow)
                        print('Not yet done for the day : ' + str(date))
                        print('New start time : ' + str(startTime))
                        print('New end time : ' + str(endTime))
                    else:
                        print('Fetching trades that are to exit by the day end')
                        resultTradesExit = dbObject.getTrainingTradesExitEnd(portfolioId, date, lastCheckedTime)
                        for id, stock, type, qty, entry_price, exit_price in resultTradesExit:
                            freedAsset = 0
                            if type==1:
                                freedAsset = qty*exit_price*(-1)            # Long Trade
                            else:
                                freedAsset = qty*(2*entry_price - exit_price)*(-1)          # Short Trade
                            dbObject.updateTrainingIndividualAsset(portfolioId, gv.dummyIndividualId, gv.dummyStockId, freedAsset)
                            dbObject.updateTrainingIndividualAsset(portfolioId, id, stock, freedAsset)
                        print('Checking if we have reached the end of testing period')
                        if(date>=periodEndDate):
                            done = True
                        else:
                            date = date + timedelta(days=1)
                            startTime = timedelta(hours=9, minutes=15)
                            endTime = timedelta(hours=10, minutes=30)
                            lastCheckedTime = timedelta(hours=9, minutes=15)
                            print('Going to next day')
                            print(datetime.now())
                            print('New day : ' + str(date))
                            print('New start time : ' + str(startTime))
                            print('New end time : ' + str(endTime))
                else:
                    date = date + timedelta(days=1)
                    if(date>periodEndDate):
                        done = True
        print('Done Training ----------------------')