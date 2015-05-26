__author__ = 'Ciddhi'

from DatabaseManager import *
from sqlalchemy import create_engine
import GlobalVariables as gv
from random import randint, sample
import logging

class DBUtils:

    databaseObject = None

    def dbConnect (self):
        db_username = gv.userName
        db_password = gv.password
        db_host = gv.dbHost
        db_name = gv.databaseName
        db_port = gv.dbPort
        db_connector = gv.dbConnector
        global databaseObject
        databaseObject = DatabaseManager(db_connector, db_username, db_password,db_host,db_port, db_name)
        databaseObject.Connect()

    def dbQuery (self, query):
        global databaseObject
        return databaseObject.Execute(query)

    def dbClose (self):
        global databaseObject
        databaseObject.Close()

    # Function to return the count of Elite individuals in table feeder_individual_table
    def getEliteCount(self, walkforward):
        global databaseObject
        query = "SELECT COUNT(*),1 FROM feeder_individual_table WHERE category=1 AND " \
                "walk_forward=" + str(walkforward)
        return databaseObject.Execute(query)

    # Function to return the count of Feasible individuals in table feeder_individual_table
    def getFeasibleCount(self, walkforward):
        global databaseObject
        query = "SELECT COUNT(*),1 FROM feeder_individual_table WHERE category=2 AND " \
                "walk_forward=" + str(walkforward)
        return databaseObject.Execute(query)

    # Function to return the count of Non-Feasible individuals in table feeder_individual_table
    def getNonFeasibleCount(self, walkforward):
        global databaseObject
        query = "SELECT COUNT(*),1 FROM feeder_individual_table WHERE category=3 AND walk_forward=" + str(walkforward)
        return databaseObject.Execute(query)

    # Function to return the count of Non-Feasible individuals in table feeder_individual_table
    def getNonFeasibleCountStock(self, walkforward, stockId):
        global databaseObject
        query = "SELECT COUNT(*),1 FROM feeder_individual_table WHERE category=3 AND stock_id=" + str(stockId) + \
                " AND walk_forward=" + str(walkforward)
        return databaseObject.Execute(query)

    # Function to return a random elite individual id
    # The provided offset should be within limit depending upon count of elite individuals
    def getRandomEliteIndividual(self, offset, walkforward):
        global databaseObject
        queryIndividual = "SELECT individual_id, stock_id FROM feeder_individual_table WHERE category=1 AND" \
                          " walk_forward=" + str(walkforward) + " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(queryIndividual)

    # Function to return a random feasible individual id.
    # The provided offset should be within limit depending upon count of feasible individuals
    def getRandomFeasibleIndividual(self, offset, walkforward):
        global databaseObject
        queryIndividual = "SELECT individual_id, stock_id FROM feeder_individual_table WHERE category=2 AND" \
                          " walk_forward=" + str(walkforward) + " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(queryIndividual)

    # Function to return a random non-feasible individual id
    # The provided offset should be within limit depending upon count of non-feasible individuals
    def getRandomNonFeasibleIndividual(self, offset, walkforward):
        global databaseObject
        queryIndividual = "SELECT individual_id, stock_id FROM feeder_individual_table WHERE category=3 AND" \
                          " walk_forward=" + str(walkforward) + " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(queryIndividual)

    # Function to return a random non-feasible individual id from a given stock
    # The provided offset should be within limit depending upon count of non-feasible individuals
    def getRandomNonFeasibleIndividualStock(self, offset, walkforward, stockId):
        global databaseObject
        queryIndividual = "SELECT individual_id, stock_id FROM feeder_individual_table WHERE category=3 AND" \
                          " walk_forward=" + str(walkforward) + " AND stock_id=" + str(stockId) + " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(queryIndividual)

    def getRandomPortfolioIndividual(self, metaIndividualId, offset):
        global databaseObject
        query = "SELECT feeder_individual_id, stock_id FROM mapping_table WHERE meta_individual_id=" + \
                str(metaIndividualId) + " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(query)

    # Function to add a new Portfolio mapping in mapping_table
    def insertPortfolioMapping(self, metaIndividualId, feederIndividualId, stockId):
        global databaseObject
        query = "INSERT INTO mapping_table" \
                " (meta_individual_id, feeder_individual_id, stock_id)" \
                " VALUES" \
                " ( " + str(metaIndividualId) + ", " + str(feederIndividualId) + ", " + str(stockId) + " )"
        return databaseObject.Execute(query)

    # Function to add new portfolio in portfolio_table
    def insertPortfolio(self, metaIndividualId, firstGeneration, lastGeneration):
        global databaseObject
        query = "INSERT INTO portfolio_table" \
                " (meta_individual_id, first_generation, last_generation)" \
                " VALUES" \
                " ( " + str(metaIndividualId) + ", " + str(firstGeneration) + ", " + str(lastGeneration) + " )"
        return databaseObject.Execute(query)

    # Function to insert a new individual in a portfolio with a single change in mapping
    def insertMutationPortfolio(self, metaIndividualId, oldFeederIndividualId, newFeederIndividualId, oldStockId, newStockId, generation):
        global databaseObject
        queryCurrent = "SELECT feeder_individual_id, stock_id FROM mapping_table WHERE meta_individual_id=" + str(metaIndividualId)
        resultCurrent = databaseObject.Execute(queryCurrent)
        queryNewMetaId = "SELECT MAX(meta_individual_id), 1 FROM mapping_table"
        resultNewId = databaseObject.Execute(queryNewMetaId)
        for newMetaId, dummy in resultNewId:
            for feederIndividualId, stock in resultCurrent:
                if feederIndividualId==oldFeederIndividualId and stock==oldStockId:
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newMetaId+1) + ", " + str(newFeederIndividualId) + ", " + str(newStockId) + " )"
                    databaseObject.Execute(query)
                else:
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newMetaId+1) + ", " + str(feederIndividualId) + ", " + str(stock) + " )"
                    databaseObject.Execute(query)
            logging.info(str(newMetaId+1) + " generated by mutation of " + str(metaIndividualId))
            query = "INSERT INTO portfolio_table" \
                    " (meta_individual_id, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newMetaId+1) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)
            return newMetaId+1

    def getFeederIndividualsWalkforward(self, walkforward):
        global databaseObject
        query = "SELECT individual_id, 1 FROM feeder_individual_table WHERE walk_forward=" + str(walkforward)
        return databaseObject.Execute(query)

    # Function to get portfolio ids in given generation from mapping_table
    def getPortfolios(self, generation):
        global databaseObject
        query = "SELECT meta_individual_id, COUNT(*) FROM mapping_table WHERE meta_individual_id IN " \
                "(SELECT meta_individual_id FROM portfolio_table WHERE last_generation=" + str(generation)\
                + " AND feasible_by_performance=1 AND feasible_by_exposure=1) GROUP BY meta_individual_id"
        return databaseObject.Execute(query)

    # Function to get size of a portfolio in given generation, specified by offset for id
    def getPortfolioSizeByOffset(self, metaPortfolioIdOffset, generation):
        global databaseObject
        queryId = "SELECT meta_individual_id, 1 FROM portfolio_table WHERE last_generation=" + str(generation) + \
                  " AND feasible_by_performance=1 AND feasible_by_exposure=1 LIMIT 1 OFFSET " + str(metaPortfolioIdOffset)
        resultId = databaseObject.Execute(queryId)
        for metaPortfolioId, dummy in resultId:
            query = "SELECT COUNT(*), meta_individual_id FROM mapping_table WHERE meta_individual_id=" + str(metaPortfolioId)
            return databaseObject.Execute(query)

    # Function to get size of a given portfolio
    def getPortfolioSize(self, portfolioId):
        global databaseObject
        query = "SELECT COUNT(*), 1 FROM mapping_table WHERE meta_individual_id=" + str(portfolioId)
        return databaseObject.Execute(query)

    # Function to insert children corresponding to crossover
    def insertCrossoverPortfolio(self, crossoverType, numChildren, id1, cut11, cut12, id2, cut21, cut22, generation):
        global databaseObject
        queryCurrent1 = "SELECT feeder_individual_id, stock_id FROM mapping_table WHERE meta_individual_id=" + str(id1)
        resultCurrent1 = databaseObject.Execute(queryCurrent1)
        queryCurrent2 = "SELECT feeder_individual_id, stock_id FROM mapping_table WHERE meta_individual_id=" + str(id2)
        resultCurrent2 = databaseObject.Execute(queryCurrent2)
        queryNewMetaId = "SELECT MAX(meta_individual_id), 1 FROM mapping_table"
        resultNewId = databaseObject.Execute(queryNewMetaId)

        list1 = []
        list2 = []
        newId = None
        for id, stock in resultCurrent1:
            list1.append((id, stock))
        for id, stock in resultCurrent2:
            list2.append((id, stock))
        for id, dummy in resultNewId:
            newId = id

        # Single Point Crossover
        if crossoverType==1:
            for i in range(0, cut11, 1):
                query = "INSERT INTO mapping_table" \
                        " (meta_individual_id, feeder_individual_id, stock_id)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                databaseObject.Execute(query)
            for i in range(cut21, len(list2), 1):
                query = "INSERT INTO mapping_table" \
                        " (meta_individual_id, feeder_individual_id, stock_id)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                databaseObject.Execute(query)
            query = "INSERT INTO portfolio_table" \
                    " (meta_individual_id, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+1) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)

            if numChildren==2:
                for i in range(0, cut21, 1):
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                    databaseObject.Execute(query)
                for i in range(cut11, len(list1), 1):
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                    databaseObject.Execute(query)
                logging.info(str(newId+1) + " and " + str(newId+2) + " generated from type " + str(crossoverType) +
                             " crossover of " + str(id1) + " and " + str(id2))
                query = "INSERT INTO portfolio_table" \
                        " (meta_individual_id, first_generation, last_generation)" \
                        " VALUES" \
                        " (" + str(newId+2) + ", " + str(generation) + ", " + str(generation) + ")"
                databaseObject.Execute(query)
            else:
                logging.info(str(newId+1) + " generated from type " + str(crossoverType) + " crossover of " +
                             str(id1) + " and " + str(id2))

        # Two Point Crossover
        if crossoverType==2:
            for i in range(0, cut11, 1):
                query = "INSERT INTO mapping_table" \
                        " (meta_individual_id, feeder_individual_id, stock_id)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                databaseObject.Execute(query)
            for i in range(cut21, cut22, 1):
                query = "INSERT INTO mapping_table" \
                        " (meta_individual_id, feeder_individual_id, stock_id)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                databaseObject.Execute(query)
            for i in range(cut12, len(list1), 1):
                 query = "INSERT INTO mapping_table" \
                        " (meta_individual_id, feeder_individual_id, stock_id)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                 databaseObject.Execute(query)
            query = "INSERT INTO portfolio_table" \
                    " (meta_individual_id, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+1) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)

            if numChildren==2:
                for i in range(0, cut21, 1):
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                    databaseObject.Execute(query)
                for i in range(cut11, cut12, 1):
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                    databaseObject.Execute(query)
                for i in range(cut22, len(list2), 1):
                     query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                     databaseObject.Execute(query)
                logging.info(str(newId+1) + " and " + str(newId+2) + " generated from type " + str(crossoverType) +
                             " crossover of " + str(id1) + " and " + str(id2))
                query = "INSERT INTO portfolio_table" \
                        " (meta_individual_id, first_generation, last_generation)" \
                        " VALUES" \
                        " (" + str(newId+2) + ", " + str(generation) + ", " + str(generation) + ")"
                databaseObject.Execute(query)
            else:
                logging.info(str(newId+1) + " generated from type " + str(crossoverType) + " crossover of " +
                             str(id1) + " and " + str(id2))

        #if crossoverType==3:

    # Function to insert a portfolio's performance in performance_table
    def insertPerformance(self, portfolioId, performance):
        global databaseObject
        query = "UPDATE portfolio_table SET performance=" + str(performance) + " WHERE meta_individual_id=" + str(portfolioId)
        return databaseObject.Execute(query)

    # Function to get portfolios in a given generation ordered by performance
    def getOrderedPortfolios(self, generation):
        global databaseObject
        query = "SELECT meta_individual_id, performance FROM portfolio_table WHERE last_generation=" + str(generation) + \
                " AND feasible_by_performance=1 AND feasible_by_exposure=1 ORDER BY performance DESC"
        return databaseObject.Execute(query)

    # Function to get top elite portfolios in a given generation, ordered by performance
    def getOrderedElitePortfolios(self, generation):
        global databaseObject
        query = "SELECT meta_individual_id, performance FROM portfolio_table WHERE last_generation>=" + \
                str(generation) + " AND first_generation<=" + str(generation) + \
                " AND feasible_by_performance=1 AND feasible_by_exposure=1" \
                " ORDER BY performance DESC LIMIT " + str(gv.numElites)
        return databaseObject.Execute(query)

    # Function to get top feasible portfolios in a given generation, ordered by performance
    def getOrderedFeasiblePortfolios(self, generation):
        global databaseObject
        query = "SELECT meta_individual_id, performance FROM portfolio_table WHERE last_generation=" + str(generation) + \
                " AND feasible_by_performance=1 AND feasible_by_exposure=1 ORDER BY performance DESC LIMIT " + str(gv.maxNumPortfolios)
        return databaseObject.Execute(query)

    def getOrderedFeasiblePortfoliosPerformanceRange(self, generation, minPerformance, maxPerformance):
        global databaseObject
        query = "SELECT meta_individual_id, performance FROM portfolio_table WHERE last_generation=" + str(generation) + \
                " AND feasible_by_performance=1 AND feasible_by_exposure=1 AND performance<" + str(maxPerformance) + \
                " AND performance>=" + str(minPerformance) + " ORDER BY performance DESC"
        return databaseObject.Execute(query)

    # Function to get number of feasible portfolios in a given generation
    def getOrderedFeasiblePortfolioCount(self, generation):
        global databaseObject
        query = "SELECT COUNT(*), 1 FROM portfolio_table WHERE last_generation=" + str(generation) + " AND feasible_by_performance=1 " \
                "AND feasible_by_exposure=1 ORDER BY performance DESC"
        return databaseObject.Execute(query)

    # Function to insert selected portfolios for next generation and update in current generation
    def updateSelectedPortfolio(self, portfolioId):
        global databaseObject
        queryUpdate = "UPDATE portfolio_table SET last_generation=last_generation+1 WHERE meta_individual_id=" + str(portfolioId)
        return databaseObject.Execute(queryUpdate)

    # Function to check if performance for a portfolio has already been calculated
    def checkPerformance(self, portfolioId):
        global databaseObject
        query = "SELECT EXISTS (SELECT 1 FROM portfolio_table WHERE meta_individual_id=" + str(portfolioId) + \
                 " AND performance IS NOT NULL), 1"
        return databaseObject.Execute(query)

    # Function to get feeder individuals in a portfolio in an ascending order
    def getFeederIndividualsPortfolio(self, portfolioId):
        global databaseObject
        query = "SELECT stock_id, feeder_individual_id  FROM mapping_table WHERE meta_individual_id=" + str(portfolioId) + \
                " ORDER BY stock_id, feeder_individual_id"
        return databaseObject.Execute(query)

    # Function to get final elites from the mapping table
    def getFinalElites(self):
        global databaseObject
        query = "SELECT meta_individual_id, performance FROM portfolio_table WHERE last_generation=" \
                "(SELECT MAX(last_generation) FROM portfolio_table)) " \
                "ORDER BY performance DESC LIMIT " + str(gv.numElites)
        return databaseObject.Execute(query)

    # Function to get total performance of all portfolios in a given generation
    def getTotalPerformance(self, generation):
        global databaseObject
        query = "SELECT SUM(performance), 1 FROM portfolio_table WHERE last_generation=" + str(generation) + \
                " AND feasible_by_performance=1 AND feasible_by_exposure=1"
        return databaseObject.Execute(query)

    # Function to check if a pair has already been involved in crossover in a generation
    def checkCrossoverPairs(self, id1, id2, generation):
        global databaseObject
        query = "SELECT EXISTS (SELECT 1 FROM crossover_pairs_table WHERE ((meta_individual_id_1=" + str(id1) + \
                " AND meta_individual_id_2=" + str(id2) + ") OR (meta_individual_id_1=" + str(id2) + \
                " AND meta_individual_id_2=" + str(id1) + ")) AND generation=" + str(generation) + "), 1"
        return databaseObject.Execute(query)

    # Function to insert a new crossover pair in table
    def insertCrossoverPair(self, id1, id2, generation):
        global databaseObject
        query = "INSERT INTO crossover_pairs_table" \
                " (meta_individual_id_1, meta_individual_id_2, generation)" \
                " VALUES" \
                " (" + str(id1) + ", " + str(id2) + ", " + str(generation) + ")"
        return databaseObject.Execute(query)

    # Function to update feasibility based on performance
    def updatePerformanceFeasibility(self):
        global databaseObject
        query = "UPDATE portfolio_table SET feasible_by_performance=1 WHERE performance>" + str(gv.thresholdPerformance)
        return databaseObject.Execute(query)

    # Function to update feasibility of an individual based on performance
    def updatePerformanceFeasibilityPortfolio(self, portfolioId):
        global databaseObject
        queryCheck = "SELECT performance, 1 FROM portfolio_table WHERE meta_individual_id=" + str(portfolioId)
        resultCheck = databaseObject.Execute(queryCheck)
        for performance, dummy in resultCheck:
            if performance>gv.thresholdPerformance:
                query = "UPDATE portfolio_table SET feasible_by_performance=1 WHERE meta_individual_id=" + str(portfolioId)
                databaseObject.Execute(query)
                return 1
            else:
                query = "UPDATE portfolio_table SET feasible_by_performance=0 WHERE meta_individual_id=" + str(portfolioId)
                databaseObject.Execute(query)
                return 0

    # Function to get all trades corresponding to a portfolio
    def getTradesPortfolioStock(self, portfolioId, stockId, startDate, endDate):
        global databaseObject
        query = "SELECT * FROM tradesheet_data_table WHERE individual_id IN ( SELECT feeder_individual_id" \
                " FROM mapping_table WHERE meta_individual_id=" + str(portfolioId) + " AND stock_id=" + str(stockId) + \
                " ) AND stock_id=" + str(stockId) + " AND entry_date>='" + str(startDate) + "' AND entry_date<=" + \
                str(endDate) + "' ORDER BY entry_date, entry_time, exit_date, exit_time"
        return databaseObject.Execute(query)

    # Function to get price series for a given stock for a given day and duration in between
    def getPriceSeriesDayDuration(self, stockId, date, startTime, endTime):
        global databaseObject
        query = "SELECT date, time, close FROM price_series_table WHERE stock_id=" + str(stockId) + " AND date='" + \
                str(date) + "' AND time>='" + str(startTime) + "' AND time<='" + str(endTime) + "'"
        return databaseObject.Execute(query)

    # Function to get price series for a given stock for a given day from a start time to day end
    def getPriceSeriesDayEnd(self, stockId, date, startTime):
        global databaseObject
        query = "SELECT date, time, close FROM price_series_table WHERE stock_id=" + str(stockId) + " AND date='" + \
                str(date) + "' AND time>='" + str(startTime) + "'"
        return databaseObject.Execute(query)

    # Function to get price series for a given stock for a given day from a day start time to a end time
    def getPriceSeriesDayStart(self, stockId, date, endTime):
        global databaseObject
        query = "SELECT date, time, close FROM price_series_table WHERE stock_id=" + str(stockId) + " AND date='" + \
                str(date) + "' AND time<='" + str(endTime) + "'"
        return databaseObject.Execute(query)

    # Function to get price series for a given stock for multiple days (excluding start and end dates)
    def getPriceSeriesDays(self, stockId, startDate, endDate):
        global databaseObject
        query = "SELECT date, time, close FROM price_series_table WHERE stock_id=" + str(stockId) + " AND date>'" + \
                str(startDate) + "' AND date<'" + str(endDate) + "'"
        return databaseObject.Execute(query)

    # Function to add or update stock wise exposure for a portfolio
    def addOrUpdateStockExposure(self, portfolioId, stockId, entryPrice, entryQty, tradeType, date, time, price):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM exposure_table WHERE individual_id=" + str(portfolioId) + \
                     " AND stock_id=" + str(stockId) + " AND date='" + str(date) + "' AND time='" + str(time) + "'" + "), 1"
        resultCheck = databaseObject.Execute(queryCheck)

        '''
        exposure = None
        if tradeType == 0:
            exposure = (entryPrice-price) * entryQty
        else:
            exposure = (price - entryPrice) * entryQty
        '''
        exposure = price * entryQty

        for check, dummy in resultCheck:
            if check==1:
                query = "UPDATE exposure_table SET exposure=exposure+" + str(exposure) + " WHERE individual_id=" + str(portfolioId) + \
                        " AND stock_id=" + str(stockId) + " AND date='" + str(date) + "' AND time='" + str(time) + "'"
                databaseObject.Execute(query)
            else:
                query = "INSERT INTO exposure_table" \
                        " (individual_id, stock_id, date, time, exposure)" \
                        " VALUES" \
                        " (" + str(portfolioId) + ", " + str(stockId) + ", '" + str(date) + "', '" + str(time) + "', " + str(exposure) + ")"
                databaseObject.Execute(query)

    # Function to get various stocks that have individuals in a portfolio
    def getPortfolioStocks(self, portfolioId):
        global databaseObject
        query = "SELECT DISTINCT(stock_id), 1 FROM mapping_table WHERE meta_individual_id=" + str(portfolioId)
        return databaseObject.Execute(query)

    # Delete function later
    # Function to get maximum exposure corresponding to every stock in the portfolio
    def getMaxExposurePortfolioStock(self, portfolioId):
        global databaseObject
        query = "SELECT MAX(exposure), stock_id FROM exposure_table WHERE meta_individual_id=" + str(portfolioId) + " GROUP BY stock_id"
        return databaseObject.Execute(query)

    # Function to get stock exposure in a portfolio at every time point
    def getExposurePortfolioStock(self, portfolioId, stockId):
        global databaseObject
        query = "SELECT SUM(exposure), 1 FROM exposure_table WHERE individual_id IN (SELECT feeder_individual_id FROM mapping_table " \
                "WHERE meta_individual_id=" + str(portfolioId) + " AND stock_id=" + str(stockId) + ") GROUP BY date, time"
        return databaseObject.Execute(query)

    # Function to get portfolio exposure at every time point
    def getExposurePortfolio(self, portfolioId):
        global databaseObject
        query = "SELECT SUM(exposure), 1 FROM exposure_table WHERE individual_id IN (SELECT feeder_individual_id FROM mapping_table " \
                "WHERE meta_individual_id=" + str(portfolioId) + ") GROUP BY date, time"
        return databaseObject.Execute(query)

    # Function to update exposure feasibility in portfolio_table
    def updateExposureFeasibility(self, portfolioId, feasibility):
        global databaseObject
        query = "UPDATE portfolio_table SET feasible_by_exposure=" + str(feasibility) + " WHERE meta_individual_id=" + str(portfolioId)
        return databaseObject.Execute(query)

    # Function to get all individuals from individual_table
    def getFeederIndividuals(self):
        global databaseObject
        query = "SELECT individual_id, stock_id FROM individual_table"
        return databaseObject.Execute(query)

    # Function to get all trades of an individual in a given period
    def getTradesFeederIndividuals(self, individualId, stockId, startDate, endDate):
        global databaseObject
        query = "SELECT * FROM tradesheet_data_table WHERE individual_id=" + str(individualId) + " AND stock_id=" + str(stockId) + \
                " AND entry_date<='" + str(endDate) + "' AND entry_date>='" + str(startDate) + "'"
        return databaseObject.Execute(query)

    # Function to get number of long individuals in a portfolio
    def getLongIndividualsPortfolio(self, portfolioId):
        global databaseObject
        query = "SELECT COUNT(*),1 FROM mapping_table m, individual_table i WHERE m.meta_individual_id=" + str(portfolioId) + \
                " AND i.individual_type=0 AND m.feeder_individual_id=i.individual_id AND m.stock_id=i.stock_id"
        return databaseObject.Execute(query)

    def insertBiasedCrossoverPortfolio(self, id1, id2, cut1, cut2, long1, size1, long2, size2, biasType, generation):
        global databaseObject
        queryCurrent1 = "SELECT m.feeder_individual_id, m.stock_id, i.individual_type FROM mapping_table m, individual_table i" \
                        " WHERE m.meta_individual_id=" + str(id1) + " AND m.feeder_individual_id=i.individual_id AND m.stock_id=i.stock_id"
        resultCurrent1 = databaseObject.Execute(queryCurrent1)
        queryCurrent2 = "SELECT m.feeder_individual_id, m.stock_id, i.individual_type FROM mapping_table m, individual_table i" \
                        " WHERE m.meta_individual_id=" + str(id2) + " AND m.feeder_individual_id=i.individual_id AND m.stock_id=i.stock_id"
        resultCurrent2 = databaseObject.Execute(queryCurrent2)
        queryNewMetaId = "SELECT MAX(meta_individual_id), 1 FROM mapping_table"
        resultNewId = databaseObject.Execute(queryNewMetaId)

        list1 = []
        list2 = []
        newId = None
        for id, stock, type in resultCurrent1:
            list1.append((id, stock, type))
        for id, stock, type in resultCurrent2:
            list2.append((id, stock, type))
        for id, dummy in resultNewId:
            newId = id

        # Long - Long Crossover
        if biasType==1:
            exchange1 = sample(range(long1), cut1)
            exchange2 = sample(range(long2), cut2)
            count1 = 0
            count2 = 0

            # Inserting individuals from first portfolio
            for i in range(0, size1, 1):
                if list1[i][2]==1:                      # Insert if short of its own
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                    databaseObject.Execute(query)
                else:
                    if count1 not in exchange1:             # insert if long is not to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                                   # insert in other portfolio if long is to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    count1 += 1

            # Inserting individuals from second portfolio
            for i in range(0, size2, 1):
                if list2[i][2]==1:                  # Insert if short of its own
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                    databaseObject.Execute(query)
                else:
                    if count2 not in exchange2:             # Insert if long is not to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                                   # Insert in other portfolio if long is to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    count2 += 1

            query = "INSERT INTO portfolio_table" \
                    " (meta_individual_id, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+1) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)
            query = "INSERT INTO portfolio_table" \
                    " (meta_individual_id, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+2) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)

        # Short - Short Crossover
        if biasType==3:
            short1 = size1 - long1
            short2 = size2 - long2
            exchange1 = sample(range(short1), cut1)
            exchange2 = sample(range(short2), cut2)
            count1 = 0
            count2 = 0

            # Inserting individuals from first portfolio
            for i in range(0, size1, 1):
                if list1[i][2]==0:                  # Insert if long of its own
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                    databaseObject.Execute(query)
                else:
                    if count1 not in exchange1:             # Insert if short is not to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                                   # Insert in other portfolio if short is to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    count1 += 1

            # Inserting individuals from second portfolio
            for i in range(0, size2, 1):
                if list2[i][2]==0:                  # Insert if long of its own
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                    databaseObject.Execute(query)
                else:                               # insert if short is not to be exchanged
                    if count2 not in exchange2:
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                           # Insert in other portfolio if short is to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    count2 += 1

            query = "INSERT INTO portfolio_table" \
                    " (meta_individual_id, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+1) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)
            query = "INSERT INTO portfolio_table" \
                    " (meta_individual_id, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+2) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)

        # Long - Short Crossover
        if biasType==21:
            short2 = size2 - long2
            exchange1 = sample(range(long1), cut1)
            exchange2 = sample(range(short2), cut2)
            count1 = 0
            count2 = 0

            # Inserting individuals from first portfolio
            for i in range(0, size1, 1):
                if list1[i][2]==1:              # Insert if short individual of its own
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                    databaseObject.Execute(query)
                else:
                    if count1 not in exchange1:         # Insert if long individual is not to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                               # Insert in other portfolio if long individual is to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    count1 += 1

            # Inserting individuals from second portfolio
            for i in range(0, size2, 1):
                if list2[i][2]==0:                  # Insert if long of its own
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                    databaseObject.Execute(query)
                else:
                    if count2 not in exchange2:             # Insert if short is not to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                                   # Insert in other portfolio if short is to exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    count2 += 1

            query = "INSERT INTO portfolio_table" \
                    " (meta_individual_id, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+1) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)
            query = "INSERT INTO portfolio_table" \
                    " (meta_individual_id, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+2) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)

        # Short - Long Crossover
        if biasType==22:
            short1 = size1 - long1
            exchange1 = sample(range(short1), cut1)
            exchange2 = sample(range(long2), cut2)
            count1 = 0
            count2 = 0

            # Inserting individuals from first portfolio
            for i in range(0, size1, 1):
                if list1[i][2]==0:              # Insert if long individual of its own
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                    databaseObject.Execute(query)
                else:
                    if count1 not in exchange1:         # Insert if short individual is not to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                               # Insert in other portfolio if short individual is to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    count1 += 1

            # Inserting individuals from second portfolio
            for i in range(0, size2, 1):
                if list2[i][2]==1:                  # Insert if short of its own
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, stock_id)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                    databaseObject.Execute(query)
                else:
                    if count2 not in exchange2:             # Insert if long is not to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                                   # Insert in other portfolio if long is to exchanged
                        query = "INSERT INTO mapping_table" \
                                " (meta_individual_id, feeder_individual_id, stock_id)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    count2 += 1

            query = "INSERT INTO portfolio_table" \
                    " (meta_individual_id, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+1) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)
            query = "INSERT INTO portfolio_table" \
                    " (meta_individual_id, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+2) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)
        return [newId+1, newId+2]



    ########################################################################################################################
    # Q Learning Functions follow:
    ########################################################################################################################

    # Function to reset all ranks in a portfolio to maximum for initialization
    def initializeRanks(self, portfolioId):
        global databaseObject
        queryIndividuals = "SELECT feeder_individual_id, stock_id FROM mapping_table WHERE meta_individual_id=" + str(portfolioId)
        queryCount = "SELECT COUNT(*), 1 FROM mapping_table WHERE meta_individual_id=" + str(portfolioId)
        resultCount = databaseObject.Execute(queryCount)
        resultIndividuals = databaseObject.Execute(queryIndividuals)
        for count, dummy in resultCount:
            for individualId, stockId in resultIndividuals:
                queryInsert = "INSERT INTO ranking_table" \
                              " (meta_individual_id, feeder_individual_id, stock_id, ranking)" \
                              " VALUES" \
                              " (" + str(portfolioId) + ", " + str(individualId) + ", " + str(stockId) + ", " + str(count) + ")"
                databaseObject.Execute(queryInsert)

    # Function to reset asset_allocation_table for portfolioId at the beginning
    def resetAssetAllocation(self, portfolioId, date, time):
        global databaseObject
        #databaseObject.Execute("DELETE FROM asset_allocation_table")
        databaseObject.Execute("INSERT INTO asset_allocation_table"
                               " (meta_individual_id, feeder_individual_id, stock_id, total_asset, used_asset, free_asset)"
                               " VALUES"
                               " (" + str(portfolioId) + ", " + str(gv.dummyIndividualId) + ", " + str(gv.dummyStockId) + ", " + str(round(gv.maxTotalAsset,4)) + ", 0, " + str(round(gv.maxTotalAsset,4)) + ")")
        databaseObject.Execute("INSERT INTO asset_daily_allocation_table"
                               " (meta_individual_id, date, time, total_asset)"
                               " VALUES"
                               " (" + str(portfolioId) + ", '" + str(date) + "', '" + str(time) + "', " + str(round(gv.maxTotalAsset, 4)) + ")")
        databaseObject.Execute("INSERT INTO training_asset_allocation_table"
                               " (meta_individual_id, feeder_individual_id, stock_id, total_asset, used_asset, free_asset)"
                               " VALUES"
                               " (" + str(portfolioId) + ", " + str(gv.dummyIndividualId) + ", " + str(gv.dummyStockId) + ", " + str(round(gv.trainingMaxTotalAsset,4)) + ", 0, " + str(round(gv.trainingMaxTotalAsset,4)) + ")")

    # Function to reset latest_individual_table every walk-forward
    def resetLatestIndividualsWalkForward(self, portfolioId):
        global databaseObject
        queryReset = "DELETE FROM latest_individual_table WHERE meta_individual_id=" + str(portfolioId)
        databaseObject.Execute(queryReset)

    # Function to reset training_asset_allocation_table every training period
    def resetAssetTraining(self, portfolioId):
        global databaseObject
        databaseObject.Execute("DELETE FROM training_asset_allocation_table WHERE meta_individual_id=" + str(portfolioId))
        databaseObject.Execute("INSERT INTO training_asset_allocation_table"
                               " (meta_individual_id, feeder_individual_id, stock_id, total_asset, used_asset, free_asset)"
                               " VALUES"
                               " (" + str(portfolioId) + ", " + str(gv.dummyIndividualId) + ", " + str(gv.dummyStockId) + ", " + str(round(gv.trainingMaxTotalAsset,4)) + ", 0, " + str(round(gv.trainingMaxTotalAsset,4)) + ")")

    # Function to get individuals from mapping_table for the portfolio
    def getRefIndividuals(self, portfolioId):
        global databaseObject
        queryIndividuals = "SELECT feeder_individual_id, stock_id FROM mapping_table WHERE meta_individual_id=" + str(portfolioId)
        return databaseObject.Execute(queryIndividuals)

    # Function to update rank of an individual in a portfolio
    def updateRank(self, portfolioId, feederIndividualId, stockId, rank):
        global databaseObject
        queryUpdate = "UPDATE ranking_table SET ranking=" + str(rank) + " WHERE meta__individual_id=" + str(portfolioId) + \
                      " AND feeder_individual_id=" + str(feederIndividualId) + " AND stock_id=" + str(stockId)
        return databaseObject.Execute(queryUpdate)

    # TODO - Test the query and result
    # Function to get new trades from original tradesheet based on ranking of
    def getRankedTradesOrdered (self, portfolioId, date, startTime, endTime):
        global databaseObject
        queryIndividuals = "SELECT feeder_individual_id, stock_id FROM mapping_table WHERE meta_individual_id=" + str(portfolioId)
        resultIndividuals = databaseObject.Execute(queryIndividuals)

        queryTrades = "SELECT t.* FROM old_tradesheet_data_table t, ranking_table r WHERE t.individual_id=r.individual_id AND t.stock_id=r.stock_id" \
                      " AND t.entry_date='" + str(date) + "' AND t.entry_time<'" + str(endTime) + "' AND t.entry_time>='" + str(startTime) + \
                      "' AND r.meta_individual_id=" + str(portfolioId) + "("
        individualCount = 0
        for feederId, stockId in resultIndividuals:
            if individualCount==0:
                queryTrades += " ( t.individual_id=" + str(feederId) + " AND t.stock_id=" + str(stockId) + " ) "
            else:
                queryTrades += " OR ( t.individual_id=" + str(feederId) + " AND t.stock_id=" + str(stockId) + " ) "
            individualCount += 1
        queryTrades +=  ") ORDER BY t.entry_time, r.ranking"
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit in a given interval
    def getTradesExit(self, portfolioId, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT individual_id, stock_id, trade_type, entry_qty, entry_price, exit_price FROM tradesheet_data_table WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "' AND exit_time<'" + str(endTime) + "' AND meta_individual_id=" + str(portfolioId)
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit at day end
    def getTradesExitEnd(self, portfolioId, date, startTime):
        global databaseObject
        queryTrades = "SELECT individual_id, stock_id, trade_type, entry_qty, entry_price, exit_price FROM tradesheet_data_table WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "' AND meta_individual_id=" + str(portfolioId)
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit in a given interval during training
    def getTrainingTradesExit(self, portfolioId, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT individual_id, stock_id, trade_type, entry_qty, entry_price, exit_price FROM training_tradesheet_data_table WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "' AND exit_time<'" + str(endTime) + "' AND meta_individual_id=" + str(portfolioId)
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit at day end during training
    def getTrainingTradesExitEnd(self, portfolioId, date, startTime):
        global databaseObject
        queryTrades = "SELECT individual_id, stock_id, trade_type, entry_qty, entry_price, exit_price FROM training_tradesheet_data_table WHERE exit_date='" + str(date) + \
                      "' AND exit_time>='" + str(startTime) + "' AND meta_individual_id=" + str(portfolioId)
        return databaseObject.Execute(queryTrades)

    # Function to update individual's asset
    def updateIndividualAsset(self, portfolioId, feederIndividualId, stockId, toBeUsedAsset):
        global databaseObject
        queryOldAsset = "SELECT total_asset, used_asset, free_asset FROM asset_allocation_table WHERE meta_individual_id=" + str(portfolioId) + \
                        " AND feeder_individual_id=" + str(feederIndividualId) + " AND stock_id=" + str(stockId)
        resultOldAsset = databaseObject.Execute(queryOldAsset)
        for totalAsset, usedAsset, freeAsset in resultOldAsset:
            newUsedAsset = float(usedAsset) + toBeUsedAsset
            newFreeAsset = float(freeAsset) - toBeUsedAsset
            queryUpdate = "UPDATE asset_allocation_table SET used_asset=" + str(round(newUsedAsset,4)) + ", free_asset=" + str(round(newFreeAsset,4)) + \
                          " WHERE meta_individual_id=" + str(portfolioId) + " AND feeder_individual_id=" + str(feederIndividualId) + " AND stock_id=" + str(stockId)
            return databaseObject.Execute(queryUpdate)

    # Function to update individual's asset during training
    def updateTrainingIndividualAsset(self, portfolioId, feederIndividualId, stockId, toBeUsedAsset):
        global databaseObject
        queryOldAsset = "SELECT total_asset, used_asset, free_asset FROM training_asset_allocation_table WHERE meta_individual_id=" + str(portfolioId) + \
                        " AND feeder_individual_id=" + str(feederIndividualId) + " AND stock_id=" + str(stockId)
        resultOldAsset = databaseObject.Execute(queryOldAsset)
        for totalAsset, usedAsset, freeAsset in resultOldAsset:
            newUsedAsset = float(usedAsset) + toBeUsedAsset
            newFreeAsset = float(freeAsset) - toBeUsedAsset
            queryUpdate = "UPDATE training_asset_allocation_table SET used_asset=" + str(round(newUsedAsset,4)) + ", free_asset=" + str(round(newFreeAsset,4)) + \
                          " WHERE meta_individual_id=" + str(portfolioId) + " AND feeder_individual_id=" + str(feederIndividualId) + " AND stock_id=" + str(stockId)
            return databaseObject.Execute(queryUpdate)

    # Function to get current free asset for an individual
    def getFreeAsset(self, portfolioId, feederIndividualId, stockId):
        global databaseObject
        queryCheck = "SELECT free_asset, 1 FROM asset_allocation_table WHERE meta_individual_id=" + str(portfolioId) + " AND feeder_individual_id=" + \
                     str(feederIndividualId) + " AND stock_id=" + str(stockId)
        return databaseObject.Execute(queryCheck)

    # Function to get current free asset for an individual during training
    def getTrainingFreeAsset(self, portfolioId, feederIndividualId, stockId):
        global databaseObject
        queryCheck = "SELECT free_asset, 1 FROM training_asset_allocation_table WHERE meta_individual_id=" + str(portfolioId) + " AND feeder_individual_id=" + \
                     str(feederIndividualId) + " AND stock_id=" + str(stockId)
        return databaseObject.Execute(queryCheck)

    # Function to check if an individual's entry exists in asset_allocation_table
    def checkIndividualAssetExists (self, portfolioId, feederIndividualId, stockId):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM asset_allocation_table WHERE meta_individual_id=" + str(portfolioId) + " AND feeder_individual_id=" + \
                     str(feederIndividualId) + " AND stock_id=" + str(stockId) + "), 0"
        return databaseObject.Execute(queryCheck)

    # Function to check if an individual's entry exists in training_asset_allocation_table
    def checkTrainingIndividualAssetExists (self, portfolioId, feederIndividualId, stockId):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM training_asset_allocation_table WHERE meta_individual_id=" + str(portfolioId) + " AND feeder_individual_id=" + \
                     str(feederIndividualId) + " AND stock_id=" + str(stockId) + "), 0"
        return databaseObject.Execute(queryCheck)

    # Function to insert individual entry in asset_allocation_table
    def addIndividualAsset (self, portfolioId, feederIndividualId, stockId, usedAsset):
        global databaseObject
        queryAddAsset = "INSERT INTO asset_allocation_table" \
                        "(meta_individual_id, feeder_individual_id, stock_id, total_asset, used_asset, free_asset)" \
                        "VALUES" \
                        "(" + str(portfolioId) + ", " + str(feederIndividualId) + ", " + str(stockId) + ", " + str(round(gv.maxAsset,4)) + ", " + str(round(usedAsset,4)) + ", " + str(round((gv.maxAsset-usedAsset),4)) + ")"
        return databaseObject.Execute(queryAddAsset)

    # Function to insert individual entry in training_asset_allocation_table
    def addTrainingIndividualAsset (self, portfolioId, feederIndividualId, stockId, usedAsset):
        global databaseObject
        queryAddAsset = "INSERT INTO training_asset_allocation_table" \
                        "(meta_individual_id, feeder_individual_id, stock_id, total_asset, used_asset, free_asset)" \
                        "VALUES" \
                        "(" + str(portfolioId) + ", " + str(feederIndividualId) + ", " + str(stockId) + ", " + str(round(gv.maxAsset,4)) + ", " + str(round(usedAsset,4)) + ", " + str(round((gv.maxAsset-usedAsset),4)) + ")"
        return databaseObject.Execute(queryAddAsset)

    # Function to insert new trade in tradesheet
    def insertNewTrade(self, stockId, individualId, entryDate, entryTime, entryPrice, exitDate, exitTime, exitPrice, entryQty, tradeType):
        global databaseObject
        queryInsertTrade = "INSERT INTO tradesheet_data_table" \
                           " (stock_id, individual_id, entry_date, entry_time, entry_price, exit_date, exit_time, exit_price, entry_qty, trade_type)" \
                           " VALUES" \
                           " (" + str(stockId) + ", " + str(individualId) + ", '" + str(entryDate) + "', '" + str(entryTime) + "', " + str(entryPrice) + \
                           ", '" + str(exitDate) + "', '" + str(exitTime) + "', " + str(exitPrice) + ", " + str(entryQty) + ", " + str(tradeType) + ")"
        databaseObject.Execute(queryInsertTrade)

    # Function to insert new trade in training_tradesheet
    def insertTrainingNewTrade(self, stockId, individualId, entryDate, entryTime, entryPrice, exitDate, exitTime, exitPrice, entryQty, tradeType):
        global databaseObject
        queryInsertTrade = "INSERT INTO training_tradesheet_data_table" \
                           " (stock_id, individual_id, entry_date, entry_time, entry_price, exit_date, exit_time, exit_price, entry_qty, trade_type)" \
                           " VALUES" \
                           " (" + str(stockId) + ", " + str(individualId) + ", '" + str(entryDate) + "', '" + str(entryTime) + "', " + str(entryPrice) + \
                           ", '" + str(exitDate) + "', '" + str(exitTime) + "', " + str(exitPrice) + ", " + str(entryQty) + ", " + str(tradeType) + ")"
        databaseObject.Execute(queryInsertTrade)

    # Function to insert individual id in latest_individual_table every walk-forward
    def insertLatestIndividual(self, portfolioId, feederIndividualId, stockId):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM latest_individual_table WHERE meta_individual_id=" + str(portfolioId) + " AND feeder_individual_id=" + \
                     str(feederIndividualId) + " AND stock_id=" + str(stockId) + "), 0"
        resultCheck = databaseObject.Execute(queryCheck)
        for check, dummy in resultCheck:
            if check==0:
                queryInsert = "INSERT INTO latest_individual_table" \
                              " (meta_individual_id, feeder_individual_id, stock_id)" \
                              " VALUES" \
                              " (" + str(portfolioId) + ", " + str(feederIndividualId) + ", " + str(stockId) + ")"
                databaseObject.Execute(queryInsert)