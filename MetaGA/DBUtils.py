__author__ = 'Ciddhi'

from DatabaseManager import *
from sqlalchemy import create_engine
import GlobalVariables as gv
from random import randint, sample
import logging
import math

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

    # Function to return the count of Elite individuals in table tblIndividualCategoryInfo
    def getEliteCount(self, walkforward):
        global databaseObject
        query = "SELECT COUNT(*),1 FROM tblIndividualCategoryInfo WHERE Category=1 AND " \
                "WalkForwardID=" + str(walkforward)
        return databaseObject.Execute(query)

    # Function to return the count of Feasible individuals in table tblIndividualCategoryInfo
    def getFeasibleCount(self, walkforward):
        global databaseObject
        query = "SELECT COUNT(*),1 FROM tblIndividualCategoryInfo WHERE Category=2 AND " \
                "WalkForwardID=" + str(walkforward)
        return databaseObject.Execute(query)

    # Function to return the count of Non-Feasible individuals in table tblIndividualCategoryInfo
    def getNonFeasibleCount(self, walkforward):
        global databaseObject
        query = "SELECT COUNT(*),1 FROM tblIndividualCategoryInfo WHERE Category=3 AND WalkForwardID=" + str(walkforward)
        return databaseObject.Execute(query)

    # Function to return the count of Non-Feasible individuals in table tblIndividualCategoryInfo
    def getNonFeasibleCountStock(self, walkforward, stockId):
        global databaseObject
        query = "SELECT COUNT(*),1 FROM tblIndividualCategoryInfo WHERE Category=3 AND SecID=" + str(stockId) + \
                " AND WalkForwardID=" + str(walkforward)
        return databaseObject.Execute(query)

    # Function to return a random elite individual id
    # The provided offset should be within limit depending upon count of elite individuals
    def getRandomEliteIndividual(self, offset, walkforward):
        global databaseObject
        queryIndividual = "SELECT IndividualID, SecID FROM tblIndividualCategoryInfo WHERE Category=1 AND" \
                          " WalkForwardID=" + str(walkforward) + " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(queryIndividual)

    # Function to return a random feasible individual id.
    # The provided offset should be within limit depending upon count of feasible individuals
    def getRandomFeasibleIndividual(self, offset, walkforward):
        global databaseObject
        queryIndividual = "SELECT IndividualID, SecID FROM tblIndividualCategoryInfo WHERE Category=2 AND" \
                          " WalkForwardID=" + str(walkforward) + " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(queryIndividual)

    # Function to return a random non-feasible individual id
    # The provided offset should be within limit depending upon count of non-feasible individuals
    def getRandomNonFeasibleIndividual(self, offset, walkforward):
        global databaseObject
        queryIndividual = "SELECT IndividualID, SecID FROM tblIndividualCategoryInfo WHERE Category=3 AND" \
                          " WalkForwardID=" + str(walkforward) + " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(queryIndividual)

    # Function to return a random non-feasible individual id from a given stock
    # The provided offset should be within limit depending upon count of non-feasible individuals
    def getRandomNonFeasibleIndividualStock(self, offset, walkforward, stockId):
        global databaseObject
        queryIndividual = "SELECT IndividualID, SecID FROM tblIndividualCategoryInfo WHERE Category=3 AND" \
                          " WalkForwardID=" + str(walkforward) + " AND SecID=" + str(stockId) + " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(queryIndividual)

    def getRandomPortfolioIndividual(self, metaIndividualId, offset):
        global databaseObject
        query = "SELECT IndividualID, SecID FROM mapping_table WHERE MetaIndividualId=" + \
                str(metaIndividualId) + " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(query)

    # Function to add a new Portfolio mapping in mapping_table
    def insertPortfolioMapping(self, metaIndividualId, feederIndividualId, stockId):
        global databaseObject
        query = "INSERT INTO mapping_table" \
                " (MetaIndividualId, IndividualID, SecID)" \
                " VALUES" \
                " ( " + str(metaIndividualId) + ", " + str(feederIndividualId) + ", " + str(stockId) + " )"
        return databaseObject.Execute(query)

    # Function to add new portfolio in portfolio_table
    def insertPortfolio(self, metaIndividualId, firstGeneration, lastGeneration):
        global databaseObject
        query = "INSERT INTO portfolio_table" \
                " (MetaIndividualId, first_generation, last_generation)" \
                " VALUES" \
                " ( " + str(metaIndividualId) + ", " + str(firstGeneration) + ", " + str(lastGeneration) + " )"
        return databaseObject.Execute(query)

    # Function to insert a new individual in a portfolio with a single change in mapping
    def insertMutationPortfolio(self, metaIndividualId, oldFeederIndividualId, newFeederIndividualId, oldStockId, newStockId, generation):
        global databaseObject
        queryCurrent = "SELECT IndividualID, SecID FROM mapping_table WHERE MetaIndividualId=" + str(metaIndividualId)
        resultCurrent = databaseObject.Execute(queryCurrent)
        queryNewMetaId = "SELECT MAX(MetaIndividualId), 1 FROM mapping_table"
        resultNewId = databaseObject.Execute(queryNewMetaId)
        for newMetaId, dummy in resultNewId:
            for feederIndividualId, stock in resultCurrent:
                if feederIndividualId==oldFeederIndividualId and stock==oldStockId:
                    query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newMetaId+1) + ", " + str(newFeederIndividualId) + ", " + str(newStockId) + " )"
                    databaseObject.Execute(query)
                else:
                    query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newMetaId+1) + ", " + str(feederIndividualId) + ", " + str(stock) + " )"
                    databaseObject.Execute(query)
            logging.info(str(newMetaId+1) + " generated by mutation of " + str(metaIndividualId))
            query = "INSERT INTO portfolio_table" \
                    " (MetaIndividualId, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newMetaId+1) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)
            return newMetaId+1

    def getFeederIndividualsWalkforward(self, walkforward):
        global databaseObject
        query = "SELECT IndividualID, 1 FROM tblIndividualCategoryInfo WHERE WalkForwardID=" + str(walkforward)
        return databaseObject.Execute(query)

    # Function to get portfolio ids in given generation from mapping_table
    def getPortfolios(self, generation):
        global databaseObject
        query = "SELECT MetaIndividualId, COUNT(*) FROM mapping_table WHERE MetaIndividualId IN " \
                "(SELECT MetaIndividualId FROM portfolio_table WHERE last_generation=" + str(generation)\
                + " AND feasible_by_performance=1) GROUP BY MetaIndividualId"
        return databaseObject.Execute(query)

    # Function to get size of a portfolio in given generation, specified by offset for id
    def getPortfolioSizeByOffset(self, metaPortfolioIdOffset, generation):
        global databaseObject
        queryId = "SELECT MetaIndividualId, 1 FROM portfolio_table WHERE last_generation=" + str(generation) + \
                  " AND feasible_by_performance=1 LIMIT 1 OFFSET " + str(metaPortfolioIdOffset)
        resultId = databaseObject.Execute(queryId)
        for metaPortfolioId, dummy in resultId:
            query = "SELECT COUNT(*), MetaIndividualId FROM mapping_table WHERE MetaIndividualId=" + str(metaPortfolioId)
            return databaseObject.Execute(query)

    # Function to get size of a given portfolio
    def getPortfolioSize(self, portfolioId):
        global databaseObject
        query = "SELECT COUNT(*), 1 FROM mapping_table WHERE MetaIndividualId=" + str(portfolioId)
        return databaseObject.Execute(query)

    # Function to insert children corresponding to crossover
    def insertCrossoverPortfolio(self, crossoverType, numChildren, id1, cut11, cut12, id2, cut21, cut22, generation):
        global databaseObject
        queryCurrent1 = "SELECT IndividualID, SecID FROM mapping_table WHERE MetaIndividualId=" + str(id1)
        resultCurrent1 = databaseObject.Execute(queryCurrent1)
        queryCurrent2 = "SELECT IndividualID, SecID FROM mapping_table WHERE MetaIndividualId=" + str(id2)
        resultCurrent2 = databaseObject.Execute(queryCurrent2)
        queryNewMetaId = "SELECT MAX(MetaIndividualId), 1 FROM mapping_table"
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
                        " (MetaIndividualId, IndividualID, SecID)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                databaseObject.Execute(query)
            for i in range(cut21, len(list2), 1):
                query = "INSERT INTO mapping_table" \
                        " (MetaIndividualId, IndividualID, SecID)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                databaseObject.Execute(query)
            query = "INSERT INTO portfolio_table" \
                    " (MetaIndividualId, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+1) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)

            if numChildren==2:
                for i in range(0, cut21, 1):
                    query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                    databaseObject.Execute(query)
                for i in range(cut11, len(list1), 1):
                    query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                    databaseObject.Execute(query)
                logging.info(str(newId+1) + " and " + str(newId+2) + " generated from type " + str(crossoverType) +
                             " crossover of " + str(id1) + " and " + str(id2))
                query = "INSERT INTO portfolio_table" \
                        " (MetaIndividualId, first_generation, last_generation)" \
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
                        " (MetaIndividualId, IndividualID, SecID)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                databaseObject.Execute(query)
            for i in range(cut21, cut22, 1):
                query = "INSERT INTO mapping_table" \
                        " (MetaIndividualId, IndividualID, SecID)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                databaseObject.Execute(query)
            for i in range(cut12, len(list1), 1):
                 query = "INSERT INTO mapping_table" \
                        " (MetaIndividualId, IndividualID, SecID)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                 databaseObject.Execute(query)
            query = "INSERT INTO portfolio_table" \
                    " (MetaIndividualId, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+1) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)

            if numChildren==2:
                for i in range(0, cut21, 1):
                    query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                    databaseObject.Execute(query)
                for i in range(cut11, cut12, 1):
                    query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                    databaseObject.Execute(query)
                for i in range(cut22, len(list2), 1):
                     query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                     databaseObject.Execute(query)
                logging.info(str(newId+1) + " and " + str(newId+2) + " generated from type " + str(crossoverType) +
                             " crossover of " + str(id1) + " and " + str(id2))
                query = "INSERT INTO portfolio_table" \
                        " (MetaIndividualId, first_generation, last_generation)" \
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
        query = "UPDATE portfolio_table SET performance=" + str(performance) + " WHERE MetaIndividualId=" + str(portfolioId)
        return databaseObject.Execute(query)

    # Function to get portfolios in a given generation ordered by performance
    def getOrderedPortfolios(self, generation):
        global databaseObject
        query = "SELECT MetaIndividualId, performance FROM portfolio_table WHERE last_generation=" + str(generation) + \
                " AND feasible_by_performance=1 ORDER BY performance DESC"
        return databaseObject.Execute(query)

    # Function to get top elite portfolios in a given generation, ordered by performance
    def getOrderedElitePortfolios(self, generation):
        global databaseObject
        query = "SELECT MetaIndividualId, performance FROM portfolio_table WHERE last_generation>=" + \
                str(generation) + " AND first_generation<=" + str(generation) + \
                " AND feasible_by_performance=1 ORDER BY performance DESC LIMIT " + str(gv.numElites)
        return databaseObject.Execute(query)

    # Function to get top feasible portfolios in a given generation, ordered by performance
    def getOrderedFeasiblePortfolios(self, generation):
        global databaseObject
        query = "SELECT MetaIndividualId, performance FROM portfolio_table WHERE last_generation=" + str(generation) + \
                " AND feasible_by_performance=1 ORDER BY performance DESC LIMIT " + str(gv.maxNumPortfolios)
        return databaseObject.Execute(query)

    def getOrderedFeasiblePortfoliosPerformanceRange(self, generation, minPerformance, maxPerformance):
        global databaseObject
        query = "SELECT MetaIndividualId, performance FROM portfolio_table WHERE last_generation=" + str(generation) + \
                " AND feasible_by_performance=1 AND performance<" + str(maxPerformance) + \
                " AND performance>=" + str(minPerformance) + " ORDER BY performance DESC"
        return databaseObject.Execute(query)

    # Function to get number of feasible portfolios in a given generation
    def getOrderedFeasiblePortfolioCount(self, generation):
        global databaseObject
        query = "SELECT COUNT(*), 1 FROM portfolio_table WHERE last_generation=" + str(generation) + " AND feasible_by_performance=1 " \
                "ORDER BY performance DESC"
        return databaseObject.Execute(query)

    # Function to insert selected portfolios for next generation and update in current generation
    def updateSelectedPortfolio(self, portfolioId):
        global databaseObject
        queryUpdate = "UPDATE portfolio_table SET last_generation=last_generation+1 WHERE MetaIndividualId=" + str(portfolioId)
        return databaseObject.Execute(queryUpdate)

    # Function to check if performance for a portfolio has already been calculated
    def checkPerformance(self, portfolioId):
        global databaseObject
        query = "SELECT EXISTS (SELECT 1 FROM portfolio_table WHERE MetaIndividualId=" + str(portfolioId) + \
                 " AND performance IS NOT NULL), 1"
        return databaseObject.Execute(query)

    # Function to get feeder individuals in a portfolio in an ascending order
    def getFeederIndividualsPortfolio(self, portfolioId):
        global databaseObject
        query = "SELECT SecID, IndividualID  FROM mapping_table WHERE MetaIndividualId=" + str(portfolioId) + \
                " ORDER BY SecID, IndividualID"
        return databaseObject.Execute(query)

    # Function to get final elites from the mapping table
    def getFinalElites(self):
        global databaseObject
        query = "SELECT MetaIndividualId, performance FROM portfolio_table WHERE last_generation=" \
                "(SELECT MAX(last_generation) FROM portfolio_table)) " \
                "ORDER BY performance DESC LIMIT " + str(gv.numElites)
        return databaseObject.Execute(query)

    # Function to get total performance of all portfolios in a given generation
    def getTotalPerformance(self, generation):
        global databaseObject
        query = "SELECT SUM(performance), 1 FROM portfolio_table WHERE last_generation=" + str(generation) + \
                " AND feasible_by_performance=1"
        return databaseObject.Execute(query)

    # Function to check if a pair has already been involved in crossover in a generation
    def checkCrossoverPairs(self, id1, id2, generation):
        global databaseObject
        query = "SELECT EXISTS (SELECT 1 FROM crossover_pairs_table WHERE ((MetaIndividualId_1=" + str(id1) + \
                " AND MetaIndividualId_2=" + str(id2) + ") OR (MetaIndividualId_1=" + str(id2) + \
                " AND MetaIndividualId_2=" + str(id1) + ")) AND generation=" + str(generation) + "), 1"
        return databaseObject.Execute(query)

    # Function to insert a new crossover pair in table
    def insertCrossoverPair(self, id1, id2, generation):
        global databaseObject
        query = "INSERT INTO crossover_pairs_table" \
                " (MetaIndividualId_1, MetaIndividualId_2, generation)" \
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
        queryCheck = "SELECT performance, 1 FROM portfolio_table WHERE MetaIndividualId=" + str(portfolioId)
        resultCheck = databaseObject.Execute(queryCheck)
        for performance, dummy in resultCheck:
            if performance>gv.thresholdPerformance:
                query = "UPDATE portfolio_table SET feasible_by_performance=1 WHERE MetaIndividualId=" + str(portfolioId)
                databaseObject.Execute(query)
                return 1
            else:
                query = "UPDATE portfolio_table SET feasible_by_performance=0 WHERE MetaIndividualId=" + str(portfolioId)
                databaseObject.Execute(query)
                return 0

    # Function to get various stocks that have individuals in a portfolio
    def getPortfolioStocks(self, portfolioId):
        global databaseObject
        query = "SELECT DISTINCT(SecID), 1 FROM mapping_table WHERE MetaIndividualId=" + str(portfolioId)
        return databaseObject.Execute(query)

    # Function to get all individuals from tblIndividualList
    def getFeederIndividuals(self):
        global databaseObject
        query = "SELECT IndividualID, SecID FROM tblIndividualList"
        return databaseObject.Execute(query)

    # Function to get number of long individuals in a portfolio
    def getLongIndividualsPortfolio(self, portfolioId):
        global databaseObject
        query = "SELECT COUNT(*),1 FROM mapping_table m, tblIndividualList i WHERE m.MetaIndividualId=" + str(portfolioId) + \
                " AND i.individual_type=1 AND m.IndividualID=i.IndividualID AND m.SecID=i.SecID"
        return databaseObject.Execute(query)

    def insertBiasedCrossoverPortfolio(self, id1, id2, cut1, cut2, long1, size1, long2, size2, biasType, generation):
        global databaseObject
        queryCurrent1 = "SELECT m.IndividualID, m.SecID, i.individual_type FROM mapping_table m, tblIndividualList i" \
                        " WHERE m.MetaIndividualId=" + str(id1) + " AND m.IndividualID=i.IndividualID AND m.SecID=i.SecID"
        resultCurrent1 = databaseObject.Execute(queryCurrent1)
        queryCurrent2 = "SELECT m.IndividualID, m.SecID, i.individual_type FROM mapping_table m, tblIndividualList i" \
                        " WHERE m.MetaIndividualId=" + str(id2) + " AND m.IndividualID=i.IndividualID AND m.SecID=i.SecID"
        resultCurrent2 = databaseObject.Execute(queryCurrent2)
        queryNewMetaId = "SELECT MAX(MetaIndividualId), 1 FROM mapping_table"
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
                if list1[i][2]==0:                      # Insert if short of its own
                    query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                    databaseObject.Execute(query)
                else:
                    if count1 not in exchange1:             # insert if long is not to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                                   # insert in other portfolio if long is to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    count1 += 1

            # Inserting individuals from second portfolio
            for i in range(0, size2, 1):
                if list2[i][2]==0:                  # Insert if short of its own
                    query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                    databaseObject.Execute(query)
                else:
                    if count2 not in exchange2:             # Insert if long is not to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                                   # Insert in other portfolio if long is to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    count2 += 1

            query = "INSERT INTO portfolio_table" \
                    " (MetaIndividualId, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+1) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)
            query = "INSERT INTO portfolio_table" \
                    " (MetaIndividualId, first_generation, last_generation)" \
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
                if list1[i][2]==1:                  # Insert if long of its own
                    query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                    databaseObject.Execute(query)
                else:
                    if count1 not in exchange1:             # Insert if short is not to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                                   # Insert in other portfolio if short is to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    count1 += 1

            # Inserting individuals from second portfolio
            for i in range(0, size2, 1):
                if list2[i][2]==1:                  # Insert if long of its own
                    query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                    databaseObject.Execute(query)
                else:                               # insert if short is not to be exchanged
                    if count2 not in exchange2:
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                           # Insert in other portfolio if short is to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    count2 += 1

            query = "INSERT INTO portfolio_table" \
                    " (MetaIndividualId, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+1) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)
            query = "INSERT INTO portfolio_table" \
                    " (MetaIndividualId, first_generation, last_generation)" \
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
                if list1[i][2]==0:              # Insert if short individual of its own
                    query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                    databaseObject.Execute(query)
                else:
                    if count1 not in exchange1:         # Insert if long individual is not to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                               # Insert in other portfolio if long individual is to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    count1 += 1

            # Inserting individuals from second portfolio
            for i in range(0, size2, 1):
                if list2[i][2]==1:                  # Insert if long of its own
                    query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                    databaseObject.Execute(query)
                else:
                    if count2 not in exchange2:             # Insert if short is not to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                                   # Insert in other portfolio if short is to exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    count2 += 1

            query = "INSERT INTO portfolio_table" \
                    " (MetaIndividualId, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+1) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)
            query = "INSERT INTO portfolio_table" \
                    " (MetaIndividualId, first_generation, last_generation)" \
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
                if list1[i][2]==1:              # Insert if long individual of its own
                    query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                    databaseObject.Execute(query)
                else:
                    if count1 not in exchange1:         # Insert if short individual is not to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                               # Insert in other portfolio if short individual is to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list1[i][0]) + ", " + str(list1[i][1]) + " )"
                        databaseObject.Execute(query)
                    count1 += 1

            # Inserting individuals from second portfolio
            for i in range(0, size2, 1):
                if list2[i][2]==0:                  # Insert if short of its own
                    query = "INSERT INTO mapping_table" \
                            " (MetaIndividualId, IndividualID, SecID)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                    databaseObject.Execute(query)
                else:
                    if count2 not in exchange2:             # Insert if long is not to be exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+2) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    else:                                   # Insert in other portfolio if long is to exchanged
                        query = "INSERT INTO mapping_table" \
                                " (MetaIndividualId, IndividualID, SecID)" \
                                " VALUES" \
                                " ( " + str(newId+1) + ", " + str(list2[i][0]) + ", " + str(list2[i][1]) + " )"
                        databaseObject.Execute(query)
                    count2 += 1

            query = "INSERT INTO portfolio_table" \
                    " (MetaIndividualId, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+1) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)
            query = "INSERT INTO portfolio_table" \
                    " (MetaIndividualId, first_generation, last_generation)" \
                    " VALUES" \
                    " (" + str(newId+2) + ", " + str(generation) + ", " + str(generation) + ")"
            databaseObject.Execute(query)
        return [newId+1, newId+2]

    # Function to set exposure for start of the day
    def setInitialExposure(self, portfolioId, date, time):
        global databaseObject
        queryIndividuals = "SELECT IndividualID, SecID FROM mapping_table WHERE MetaIndividualId=" + str(portfolioId)
        resultIndividuals = databaseObject.Execute(queryIndividuals)
        for feederId, stockId in resultIndividuals:
            query = "INSERT INTO exposure_table" \
                    " (MetaIndividualId, IndividualID, SecID, date, time, exposure)" \
                    " VALUES" \
                    " (" + str(portfolioId) + ", " + str(feederId) + ", " + str(stockId) + ", '" + str(date) + "', '" + str(time) + "', " + str(0) + ")"
            databaseObject.Execute(query)
        return

    # Function to get daily trades for a portfolio from original tradesheet
    def getDayTrades(self, portfolioId, date):
        global databaseObject
        queryIndividuals = "SELECT IndividualID, SecID FROM mapping_table WHERE MetaIndividualId=" + str(portfolioId)
        resultIndividuals = databaseObject.Execute(queryIndividuals)
        query = "SELECT * FROM  tblIndividualTradesheet WHERE EntryDate='" + str(date) + "' AND ("
        individualCount = 0
        for feederId, stockId in resultIndividuals:
            if individualCount==0:
                query = query + " ( IndividualID=" + str(feederId) + " AND SecID=" + str(stockId) + ") "
            else:
                query = query + " OR ( IndividualID=" + str(feederId) + " AND SecID=" + str(stockId) + ") "
            individualCount += 1
        query = query + ") ORDER BY EntryTime"
        return databaseObject.Execute(query)

    # Function to update current exposure for all feeder individuals in a portfolio and return total exposure and stock exposure
    def updateAndGetCurrentExposure(self, portfolioId, stockId, date, time):
        global databaseObject
        queryIndividuals = "SELECT IndividualID, SecID FROM mapping_table WHERE MetaIndividualId=" + str(portfolioId)
        resultIndividuals = databaseObject.Execute(queryIndividuals)
        queryCheck = "SELECT EXISTS ( SELECT 1 FROM exposure_table WHERE MetaIndividualId=" + str(portfolioId) + " AND date='" + str(date) + "' AND time='" + str(time) + "' ) , 1"
        resultCheck = databaseObject.Execute(queryCheck)
        totalExposure = 0
        stockExposure = 0
        for check, dummy in resultCheck:
            if check==0:
                for feederId, stock in resultIndividuals:
                    exposure = 0
                    longQty = None
                    shortQty = None
                    price = None
                    queryLongQty = "SELECT SUM(Qty), 1 FROM portfolio_tradesheet_data_table WHERE MetaIndividualId=" + str(portfolioId) + \
                                   " AND IndividualID=" + str(feederId) + " AND SecID=" + str(stock) + " AND EntryDate='" + str(date) + \
                                   "' AND EntryTime<='" + str(time) + "' AND ExitTime>'" + str(time) + "' AND TradeType=1"
                    queryShortQty = "SELECT SUM(Qty), 1 FROM portfolio_tradesheet_data_table WHERE MetaIndividualId=" + str(portfolioId) + \
                                   " AND IndividualID=" + str(feederId) + " AND SecID=" + str(stock) + " AND EntryDate='" + str(date) + \
                                   "' AND EntryTime<='" + str(time) + "' AND ExitTime>'" + str(time) + "' AND TradeType=0"
                    queryPrice = "SELECT Open, 1 FROM tblStockPriceData WHERE SecID=" + str(stock) + " AND PriceDate='" + str(date) + "' AND PriceTime='" + str(time) + "'"
                    resultLongQty = databaseObject.Execute(queryLongQty)
                    resultShortQty = databaseObject.Execute(queryShortQty)
                    resultPrice = databaseObject.Execute(queryPrice)
                    for qty, dummy1 in resultLongQty:
                        longQty = qty
                    for qty, dummy1 in resultShortQty:
                        shortQty = qty
                    for p, dummy in resultPrice:
                        price = p
                    if (longQty or shortQty) and price:
                        if longQty:
                            exposure += float(longQty) * price
                        if shortQty:
                            exposure += float(shortQty) * price * (-1)
                    queryInsert = "INSERT INTO exposure_table" \
                                  " ( MetaIndividualId, IndividualID, SecID, date, time, exposure )" \
                                  " VALUES" \
                                  " ( " + str(portfolioId) + ", " + str(feederId) + ", " + str(stock) + ", '" + str(date) + "', '" + str(time) + "', " + str(exposure) + " )"
                    databaseObject.Execute(queryInsert)
                    totalExposure += exposure
                    if stock == stockId:
                        stockExposure += exposure
            else:
                queryTotal = "SELECT SUM(exposure), 1 FROM exposure_table WHERE MetaIndividualId=" + str(portfolioId) + " AND date='" + str(date) + \
                             "' AND time='" + str(time) + "'"
                queryStock = "SELECT SUM(exposure), 1 FROM exposure_table WHERE MetaIndividualId=" + str(portfolioId) + " AND SecID=" + str(stockId) + \
                             " AND date='" + str(date) + "' AND time='" + str(time) + "'"
                resultTotal = databaseObject.Execute(queryTotal)
                resultStock = databaseObject.Execute(queryStock)
                for total, dummy in resultTotal:
                    totalExposure = total
                for stock, dummy in resultStock:
                    stockExposure = stock
        return [totalExposure, stockExposure]

    # Function to get current exposure without updating in database
    def getCurrentExposure(self, portfolioId, stockId, date, time):
        global databaseObject
        queryIndividuals = "SELECT IndividualID, SecID FROM mapping_table WHERE MetaIndividualId=" + str(portfolioId)
        resultIndividuals = databaseObject.Execute(queryIndividuals)
        totalExposure = 0
        stockExposure = 0
        for feederId, stock in resultIndividuals:
            exposure = 0
            longQty = None
            shortQty = None
            price = None
            queryLongQty = "SELECT SUM(Qty), 1 FROM portfolio_tradesheet_data_table WHERE MetaIndividualId=" + str(portfolioId) + \
                           " AND IndividualID=" + str(feederId) + " AND SecID=" + str(stock) + " AND EntryDate='" + str(date) + \
                           "' AND EntryTime<='" + str(time) + "' AND ExitTime>'" + str(time) + "' AND TradeType=1"
            queryShortQty = "SELECT SUM(Qty), 1 FROM portfolio_tradesheet_data_table WHERE MetaIndividualId=" + str(portfolioId) + \
                           " AND IndividualID=" + str(feederId) + " AND SecID=" + str(stock) + " AND EntryDate='" + str(date) + \
                           "' AND EntryTime<='" + str(time) + "' AND ExitTime>'" + str(time) + "' AND TradeType=0"
            queryPrice = "SELECT Open, 1 FROM tblStockPriceData WHERE SecID=" + str(stock) + " AND PriceDate='" + str(date) + "' AND PriceTime='" + str(time) + "'"
            resultLongQty = databaseObject.Execute(queryLongQty)
            resultShortQty = databaseObject.Execute(queryShortQty)
            resultPrice = databaseObject.Execute(queryPrice)
            for qty, dummy1 in resultLongQty:
                longQty = qty
            for qty, dummy1 in resultShortQty:
                shortQty = qty
            for p, dummy in resultPrice:
                price = p
            if (longQty or shortQty) and price:
                if longQty:
                    exposure += float(longQty) * price
                if shortQty:
                    exposure += float(shortQty) * price * (-1)
            totalExposure += exposure
            if stock == stockId:
                stockExposure += exposure
        return [totalExposure, stockExposure]

    # Function to update exposure corresponding to new trade taken
    def updateNewExposure(self, portfolioId, feederIndividualId, stockId, date, time, exposure):
        global databaseObject
        query = "UPDATE exposure_table SET exposure=exposure+" + str(exposure) + " WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND date='" + str(date) + "' AND time='" + str(time) + "'"
        return databaseObject.Execute(query)

    # Function to insert new trade for a portfolio
    def insertTrade(self, portfolioId, stockId, feederIndividualId, entryDate, entryTime, entryPrice, exitDate, exitTime, exitPrice, entryQty, tradeType):
        global databaseObject
        query = "INSERT INTO portfolio_tradesheet_data_table" \
                " (MetaIndividualId, SecID, IndividualID, EntryDate, EntryTime, EntryPrice, ExitDate, ExitTime, ExitPrice, Qty, TradeType)" \
                " VALUES" \
                " (" + str(portfolioId) + ", " + str(stockId) + ", " + str(feederIndividualId) + ", '" + str(entryDate) + "', '" + str(entryTime) + \
                "', "+ str(entryPrice) + ", '" + str(exitDate) + "', '" + str(exitTime) + "', " + str(exitPrice) + ", " + str(entryQty) + ", " + str(tradeType) + ")"
        return databaseObject.Execute(query)

    # Function to update performance of a feeder individual
    def updateFeederIndividualPerformance(self, feederIndividualId, stockId, performance, walkforward):
        global databaseObject
        query = "INSERT INTO feeder_performance_table" \
                " (IndividualID, SecID, performance, WalkForwardID)" \
                " VALUES" \
                " (" + str(feederIndividualId) + ", " + str(stockId) + ", " + str(performance) + ", " + str(walkforward) + ")"
        return databaseObject.Execute(query)

    # Function to update category of feeder_individuals depending upon top percentage
    def updateCategory(self, walkforward):
        global databaseObject
        queryInsert = "INSERT INTO tblIndividualCategoryInfo (IndividualID, SecID, WalkForwardID)" \
                      "SELECT IndividualID, SecID, WalkForwardID FROM feeder_performance_table"
        databaseObject.Execute(queryInsert)

        queryNonFeasible = "UPDATE tblIndividualCategoryInfo ct " \
                        "JOIN feeder_performance_table pt " \
                        "ON ct.IndividualID=pt.IndividualID AND ct.SecID=pt.SecID AND ct.WalkForwardID=pt.WalkForwardID " \
                        "SET Category=3 " \
                        "WHERE pt.performance<0"
        databaseObject.Execute(queryNonFeasible)

        queryTotal = "SELECT COUNT(*), 1 FROM feeder_performance_table WHERE WalkForwardID=" + str(walkforward)
        numTotal = None
        numElites = None
        resultTotal = databaseObject.Execute(queryTotal)
        for count, dummy in resultTotal:
            numTotal = count
        numElites = int(math.ceil(gv.fractionElites * numTotal))

        queryTopIndividuals = "SELECT IndividualID, SecID FROM feeder_performance_table WHERE WalkForwardID=" + str(walkforward) + \
                              " AND performance>0 ORDER BY performance DESC LIMIT " + str(numElites)
        resultElites = databaseObject.Execute(queryTopIndividuals)
        for feederId, stockId in resultElites:
            queryElite = "UPDATE tblIndividualCategoryInfo SET Category=1 WHERE IndividualID=" + str(feederId) + " AND SecID=" + str(stockId) + \
                         " AND WalkForwardID=" + str(walkforward)
            databaseObject.Execute(queryElite)

        queryFeasible = "UPDATE tblIndividualCategoryInfo SET Category=2 WHERE WalkForwardID=" + str(walkforward) + " AND Category IS NULL"
        return databaseObject.Execute(queryFeasible)

    def updateFeederIndividualCategory(self, feederIndividualId, stockId, category, walkforward):
        global databaseObject
        query = "INSERT INTO tblIndividualCategoryInfo " \
                "(IndividualID, SecID, WalkForwardID, Category)" \
                " VALUES" \
                " (" + str(feederIndividualId) + ", " + str(stockId) + ", " + str(walkforward)+ ", " + str(category) + ")"
        return databaseObject.Execute(query)

