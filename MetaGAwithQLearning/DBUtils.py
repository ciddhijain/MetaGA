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

    ########################################################################################################################
    # Q Learning Functions follow:
    ########################################################################################################################

    # Function to reset all ranks in a portfolio to maximum for initialization
    def initializeRanks(self, portfolioId):
        global databaseObject
        queryIndividuals = "SELECT IndividualID, SecID FROM mapping_table WHERE MetaIndividualId=" + str(portfolioId)
        queryCount = "SELECT COUNT(*), 1 FROM mapping_table WHERE MetaIndividualId=" + str(portfolioId)
        resultCount = databaseObject.Execute(queryCount)
        resultIndividuals = databaseObject.Execute(queryIndividuals)
        for count, dummy in resultCount:
            for individualId, stockId in resultIndividuals:
                queryInsert = "INSERT INTO ranking_table" \
                              " (MetaIndividualId, IndividualID, SecID, ranking)" \
                              " VALUES" \
                              " (" + str(portfolioId) + ", " + str(individualId) + ", " + str(stockId) + ", " + str(count) + ")"
                databaseObject.Execute(queryInsert)

    # Function to reset asset_allocation_table for portfolioId at the beginning
    def resetAssetAllocation(self, portfolioId, date, time):
        global databaseObject
        #databaseObject.Execute("DELETE FROM asset_allocation_table")
        databaseObject.Execute("INSERT INTO asset_allocation_table"
                               " (MetaIndividualId, IndividualID, SecID, total_asset, used_asset, free_asset)"
                               " VALUES"
                               " (" + str(portfolioId) + ", " + str(gv.dummyIndividualId) + ", " + str(gv.dummyStockId) + ", " + str(round(gv.maxTotalAsset,4)) + ", 0, " + str(round(gv.maxTotalAsset,4)) + ")")
        databaseObject.Execute("INSERT INTO asset_daily_allocation_table"
                               " (MetaIndividualId, date, time, total_asset)"
                               " VALUES"
                               " (" + str(portfolioId) + ", '" + str(date) + "', '" + str(time) + "', " + str(round(gv.maxTotalAsset, 4)) + ")")
        databaseObject.Execute("INSERT INTO training_asset_allocation_table"
                               " (MetaIndividualId, IndividualID, SecID, total_asset, used_asset, free_asset)"
                               " VALUES"
                               " (" + str(portfolioId) + ", " + str(gv.dummyIndividualId) + ", " + str(gv.dummyStockId) + ", " + str(round(gv.trainingMaxTotalAsset,4)) + ", 0, " + str(round(gv.trainingMaxTotalAsset,4)) + ")")

    # Function to reset latest_tblIndividualList every walk-forward
    def resetLatestIndividualsWalkForward(self, portfolioId):
        global databaseObject
        queryReset = "DELETE FROM latest_tblIndividualList WHERE MetaIndividualId=" + str(portfolioId)
        databaseObject.Execute(queryReset)

    # Function to reset training_asset_allocation_table every training period
    def resetAssetTraining(self, portfolioId):
        global databaseObject
        databaseObject.Execute("DELETE FROM training_asset_allocation_table WHERE MetaIndividualId=" + str(portfolioId))
        databaseObject.Execute("INSERT INTO training_asset_allocation_table"
                               " (MetaIndividualId, IndividualID, SecID, total_asset, used_asset, free_asset)"
                               " VALUES"
                               " (" + str(portfolioId) + ", " + str(gv.dummyIndividualId) + ", " + str(gv.dummyStockId) + ", " + str(round(gv.trainingMaxTotalAsset,4)) + ", 0, " + str(round(gv.trainingMaxTotalAsset,4)) + ")")

    # Function to get individuals from mapping_table for the portfolio
    def getPortfolioIndividuals(self, portfolioId):
        global databaseObject
        queryIndividuals = "SELECT IndividualID, SecID FROM mapping_table WHERE MetaIndividualId=" + str(portfolioId)
        return databaseObject.Execute(queryIndividuals)

    # Function to update rank of an individual in a portfolio
    def updateRank(self, portfolioId, feederIndividualId, stockId, rank):
        global databaseObject
        queryUpdate = "UPDATE ranking_table SET ranking=" + str(rank) + " WHERE MetaIndividualId=" + str(portfolioId) + \
                      " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId)
        return databaseObject.Execute(queryUpdate)

    # Function to get new trades from original tradesheet based on ranking of
    def getRankedTradesOrdered (self, portfolioId, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT t.* FROM  tblIndividualTradesheet t, ranking_table r WHERE t.IndividualID=r.IndividualID AND t.SecID=r.SecID" \
                      " AND t.EntryDate='" + str(date) + "' AND t.EntryTime<'" + str(endTime) + "' AND t.EntryTime>='" + str(startTime) + \
                      "' AND r.MetaIndividualId=" + str(portfolioId) + " ORDER BY t.EntryTime, r.ranking"
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit in a given interval
    def getTradesExit(self, portfolioId, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT IndividualID, SecID, TradeType, Qty, EntryPrice, ExitPrice FROM portfolio_tradesheet_data_table WHERE ExitDate='" + str(date) + \
                      "' AND ExitTime>='" + str(startTime) + "' AND ExitTime<'" + str(endTime) + "' AND MetaIndividualId=" + str(portfolioId)
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit at day end
    def getTradesExitEnd(self, portfolioId, date, startTime):
        global databaseObject
        queryTrades = "SELECT IndividualID, SecID, TradeType, Qty, EntryPrice, ExitPrice FROM portfolio_tradesheet_data_table WHERE ExitDate='" + str(date) + \
                      "' AND ExitTime>='" + str(startTime) + "' AND MetaIndividualId=" + str(portfolioId)
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit in a given interval during training
    def getTrainingTradesExit(self, portfolioId, date, startTime, endTime):
        global databaseObject
        queryTrades = "SELECT IndividualID, SecID, TradeType, Qty, EntryPrice, ExitPrice FROM training_tradesheet_data_table WHERE ExitDate='" + str(date) + \
                      "' AND ExitTime>='" + str(startTime) + "' AND ExitTime<'" + str(endTime) + "' AND MetaIndividualId=" + str(portfolioId)
        return databaseObject.Execute(queryTrades)

    # Function to get trades that are to exit at day end during training
    def getTrainingTradesExitEnd(self, portfolioId, date, startTime):
        global databaseObject
        queryTrades = "SELECT IndividualID, SecID, TradeType, Qty, EntryPrice, ExitPrice FROM training_tradesheet_data_table WHERE ExitDate='" + str(date) + \
                      "' AND ExitTime>='" + str(startTime) + "' AND MetaIndividualId=" + str(portfolioId)
        return databaseObject.Execute(queryTrades)

    # Function to update individual's asset
    def updateIndividualAsset(self, portfolioId, feederIndividualId, stockId, toBeUsedAsset):
        global databaseObject
        queryOldAsset = "SELECT total_asset, used_asset, free_asset FROM asset_allocation_table WHERE MetaIndividualId=" + str(portfolioId) + \
                        " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId)
        resultOldAsset = databaseObject.Execute(queryOldAsset)
        for totalAsset, usedAsset, freeAsset in resultOldAsset:
            newUsedAsset = float(usedAsset) + toBeUsedAsset
            newFreeAsset = float(freeAsset) - toBeUsedAsset
            queryUpdate = "UPDATE asset_allocation_table SET used_asset=" + str(round(newUsedAsset,4)) + ", free_asset=" + str(round(newFreeAsset,4)) + \
                          " WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId)
            return databaseObject.Execute(queryUpdate)

    # Function to update individual's asset during training
    def updateTrainingIndividualAsset(self, portfolioId, feederIndividualId, stockId, toBeUsedAsset):
        global databaseObject
        queryOldAsset = "SELECT total_asset, used_asset, free_asset FROM training_asset_allocation_table WHERE MetaIndividualId=" + str(portfolioId) + \
                        " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId)
        resultOldAsset = databaseObject.Execute(queryOldAsset)
        for totalAsset, usedAsset, freeAsset in resultOldAsset:
            newUsedAsset = float(usedAsset) + toBeUsedAsset
            newFreeAsset = float(freeAsset) - toBeUsedAsset
            queryUpdate = "UPDATE training_asset_allocation_table SET used_asset=" + str(round(newUsedAsset,4)) + ", free_asset=" + str(round(newFreeAsset,4)) + \
                          " WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId)
            return databaseObject.Execute(queryUpdate)

    # Function to get current free asset for an individual
    def getFreeAsset(self, portfolioId, feederIndividualId, stockId):
        global databaseObject
        queryCheck = "SELECT free_asset, 1 FROM asset_allocation_table WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                     str(feederIndividualId) + " AND SecID=" + str(stockId)
        return databaseObject.Execute(queryCheck)

    # Function to get current free asset for an individual during training
    def getTrainingFreeAsset(self, portfolioId, feederIndividualId, stockId):
        global databaseObject
        queryCheck = "SELECT free_asset, 1 FROM training_asset_allocation_table WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                     str(feederIndividualId) + " AND SecID=" + str(stockId)
        return databaseObject.Execute(queryCheck)

    # Function to check if an individual's entry exists in asset_allocation_table
    def checkIndividualAssetExists (self, portfolioId, feederIndividualId, stockId):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM asset_allocation_table WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                     str(feederIndividualId) + " AND SecID=" + str(stockId) + "), 0"
        return databaseObject.Execute(queryCheck)

    # Function to check if an individual's entry exists in training_asset_allocation_table
    def checkTrainingIndividualAssetExists (self, portfolioId, feederIndividualId, stockId):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM training_asset_allocation_table WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                     str(feederIndividualId) + " AND SecID=" + str(stockId) + "), 0"
        return databaseObject.Execute(queryCheck)

    # Function to insert individual entry in asset_allocation_table
    def addIndividualAsset (self, portfolioId, feederIndividualId, stockId, usedAsset):
        global databaseObject
        queryAddAsset = "INSERT INTO asset_allocation_table" \
                        "(MetaIndividualId, IndividualID, SecID, total_asset, used_asset, free_asset)" \
                        "VALUES" \
                        "(" + str(portfolioId) + ", " + str(feederIndividualId) + ", " + str(stockId) + ", " + str(round(gv.maxAsset,4)) + ", " + str(round(usedAsset,4)) + ", " + str(round((gv.maxAsset-usedAsset),4)) + ")"
        return databaseObject.Execute(queryAddAsset)

    # Function to insert individual entry in training_asset_allocation_table
    def addTrainingIndividualAsset (self, portfolioId, feederIndividualId, stockId, usedAsset):
        global databaseObject
        queryAddAsset = "INSERT INTO training_asset_allocation_table" \
                        "(MetaIndividualId, IndividualID, SecID, total_asset, used_asset, free_asset)" \
                        "VALUES" \
                        "(" + str(portfolioId) + ", " + str(feederIndividualId) + ", " + str(stockId) + ", " + str(round(gv.maxAsset,4)) + ", " + str(round(usedAsset,4)) + ", " + str(round((gv.maxAsset-usedAsset),4)) + ")"
        return databaseObject.Execute(queryAddAsset)

    # Function to insert new trade in tradesheet
    def insertNewTrade(self, portfolioId, stockId, individualId, entryDate, entryTime, entryPrice, exitDate, exitTime, exitPrice, entryQty, tradeType):
        global databaseObject
        queryInsertTrade = "INSERT INTO portfolio_tradesheet_data_table" \
                           " (MetaIndividualId, SecID, IndividualID, EntryDate, EntryTime, EntryPrice, ExitDate, ExitTime, ExitPrice, Qty, TradeType)" \
                           " VALUES" \
                           " (" + str(portfolioId) + ", " + str(stockId) + ", " + str(individualId) + ", '" + str(entryDate) + "', '" + str(entryTime) + "', " + str(entryPrice) + \
                           ", '" + str(exitDate) + "', '" + str(exitTime) + "', " + str(exitPrice) + ", " + str(entryQty) + ", " + str(tradeType) + ")"
        databaseObject.Execute(queryInsertTrade)

    # Function to insert new trade in training_tradesheet
    def insertTrainingNewTrade(self, portfolioId, stockId, individualId, entryDate, entryTime, entryPrice, exitDate, exitTime, exitPrice, entryQty, tradeType):
        global databaseObject
        queryInsertTrade = "INSERT INTO training_tradesheet_data_table" \
                           " (MetaIndividualId, SecID, IndividualID, EntryDate, EntryTime, EntryPrice, ExitDate, ExitTime, ExitPrice, Qty, TradeType)" \
                           " VALUES" \
                           " (" + str(portfolioId) + ", " + str(stockId) + ", " + str(individualId) + ", '" + str(entryDate) + "', '" + str(entryTime) + "', " + str(entryPrice) + \
                           ", '" + str(exitDate) + "', '" + str(exitTime) + "', " + str(exitPrice) + ", " + str(entryQty) + ", " + str(tradeType) + ")"
        databaseObject.Execute(queryInsertTrade)

    # Function to insert individual id in latest_tblIndividualList every walk-forward
    def insertLatestIndividual(self, portfolioId, feederIndividualId, stockId):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM latest_tblIndividualList WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                     str(feederIndividualId) + " AND SecID=" + str(stockId) + "), 0"
        resultCheck = databaseObject.Execute(queryCheck)
        for check, dummy in resultCheck:
            if check==0:
                queryInsert = "INSERT INTO latest_tblIndividualList" \
                              " (MetaIndividualId, IndividualID, SecID)" \
                              " VALUES" \
                              " (" + str(portfolioId) + ", " + str(feederIndividualId) + ", " + str(stockId) + ")"
                databaseObject.Execute(queryInsert)

    # TODO - Test the query and result
    # Function to get individuals which have active trades in a given interval of time on a given day
    def getIndividuals (self, portfolioId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryIndividuals = "SELECT DISTINCT IndividualID, SecID FROM portfolio_tradesheet_data_table WHERE EntryTime<'" + str(endTime) + \
                           "' AND ExitTime>'" + str(startTime) + "' AND EntryDate='" + str(startDate) + "' AND MetaIndividualId=" + str(portfolioId)
        return databaseObject.Execute(queryIndividuals)

    # Function to get individuals which have active trades in a given interval of time on a given day during training
    def getTrainingIndividuals (self, portfolioId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryIndividuals = "SELECT DISTINCT IndividualID, SecID FROM training_tradesheet_data_table WHERE EntryTime<'" + str(endTime) + \
                           "' AND ExitTime>'" + str(startTime) + "' AND EntryDate='" + str(startDate) + "' AND MetaIndividualId=" + str(portfolioId)
        return databaseObject.Execute(queryIndividuals)

    # Function to get trades taken by an individual in an interval
    def getTradesIndividual(self, portfolioId, feederIndividualId, stockId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryTrades = "SELECT * FROM portfolio_tradesheet_data_table WHERE EntryDate='" + str(startDate) + "' AND EntryTime<='" + str(endTime) + \
                      "' AND ExitTime>='" + str(startTime) + "' AND MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" \
                      + str(feederIndividualId) + " AND SecID=" + str(stockId)
        return databaseObject.Execute(queryTrades)

    # Function to get trades taken by an individual in an interval during training
    def getTrainingTradesIndividual(self, portfolioId, feederIndividualId, stockId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryTrades = "SELECT * FROM training_tradesheet_data_table WHERE EntryDate='" + str(startDate) + "' AND EntryTime<='" + str(endTime) + \
                      "' AND ExitTime>='" + str(startTime) + "' AND MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" \
                      + str(feederIndividualId) + " AND SecID=" + str(stockId)
        return databaseObject.Execute(queryTrades)

    # Function to get price from price series for a given date and time
    def getPrice(self, stockId, startDate, startTime):
        global databaseObject
        queryPrice = "SELECT PriceTime, Open FROM tblStockPriceData WHERE PriceDate='" + str(startDate) + "' AND PriceTime='" + str(startTime) +\
                     "' AND SecID=" + str(stockId)
        return databaseObject.Execute(queryPrice)

    # Function to insert MTM value in db
    def addOrUpdateMTM(self, portfolioId, feederIndividualId, stockId, tradeType, entryDate, mtmTime, mtm):
        global databaseObject
        queryCheckRecord = "SELECT EXISTS (SELECT 1 FROM mtm_table WHERE MetaIndividualId=" + str(portfolioId) + \
                           " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + \
                           " AND TradeType=" + str(tradeType) + " AND date='" + str(entryDate) + "' AND time='" + str(mtmTime) + "'), 0"

        resultRecord = databaseObject.Execute(queryCheckRecord)
        for result, dummy in resultRecord:
            if result==0:
                queryInsertMTM = "INSERT INTO mtm_table " \
                                 "(MetaIndividualId, IndividualID, SecID, TradeType, date, time, mtm) " \
                                 "VALUES " \
                                 "(" + str(portfolioId) + ", " + str(feederIndividualId) + ", " + str(stockId) + ", " + str(tradeType) + \
                                 ", '" + str(entryDate) + "', '" + str(mtmTime) + "', " + str(mtm) + ")"
                return databaseObject.Execute(queryInsertMTM)
            else:
                queryUpdateMTM = "UPDATE mtm_table SET mtm=mtm+" + str(mtm) + " WHERE MetaIndividualId=" + str(portfolioId) + \
                                 " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + \
                                 " AND TradeType=" + str(tradeType) + " AND date='" + str(entryDate) + "' AND time='" + str(mtmTime) + "'"
                return databaseObject.Execute(queryUpdateMTM)

    # Function to insert MTM value in db during training
    def addOrUpdateTrainingMTM(self, portfolioId, feederIndividualId, stockId, tradeType, entryDate, mtmTime, mtm):
        global databaseObject
        queryCheckRecord = "SELECT EXISTS (SELECT 1 FROM training_mtm_table WHERE MetaIndividualId=" + str(portfolioId) + \
                           " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + \
                           " AND TradeType=" + str(tradeType) + " AND date='" + str(entryDate) + "' AND time='" + str(mtmTime) + "'), 0"

        resultRecord = databaseObject.Execute(queryCheckRecord)
        for result, dummy in resultRecord:
            if result==0:
                queryInsertMTM = "INSERT INTO training_mtm_table " \
                                 "(MetaIndividualId, IndividualID, SecID, TradeType, date, time, mtm) " \
                                 "VALUES " \
                                 "(" + str(portfolioId) + ", " + str(feederIndividualId) + ", " + str(stockId) + ", " + str(tradeType) + \
                                 ", '" + str(entryDate) + "', '" + str(mtmTime) + "', " + str(mtm) + ")"
                return databaseObject.Execute(queryInsertMTM)
            else:
                queryUpdateMTM = "UPDATE training_mtm_table SET mtm=mtm+" + str(mtm) + " WHERE MetaIndividualId=" + str(portfolioId) + \
                                 " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + \
                                 " AND TradeType=" + str(tradeType) + " AND date='" + str(entryDate) + "' AND time='" + str(mtmTime) + "'"
                return databaseObject.Execute(queryUpdateMTM)

    # Function to get net MTM for all long trades
    def getTotalPosMTM (self, portfolioId, feederIndividualId, stockId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryMTM = "SELECT SUM(mtm), 1 FROM mtm_table WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                   str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND time>'" + str(startTime) + "' AND date>='" + str(startDate) + \
                   "' AND date<='" + str(endDate) + "' AND time<='" + str(endTime) + "' AND TradeType=1"
        #print(queryMTM)
        return databaseObject.Execute(queryMTM)

    # function to get total quantity for all long trades
    def getTotalPosQty (self, portfolioId, feederIndividualId, stockId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryQty = "SELECT SUM(Qty), 1 FROM portfolio_tradesheet_data_table WHERE MetaIndividualId=" + str(portfolioId) + \
                   " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND EntryTime<'" + \
                   str(endTime) + "' AND ExitTime>'" + str(startTime) + "' AND EntryDate='" + str(startDate) + "' AND TradeType=1"
        #print(queryQty)
        return databaseObject.Execute(queryQty)

    # Function to get net MTM for all short trades
    def getTotalNegMTM (self, portfolioId, feederIndividualId, stockId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryMTM = "SELECT SUM(mtm), 1 FROM mtm_table WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                   str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND time>'" + str(startTime) + "' AND date>='" + str(startDate) + \
                   "' AND date<='" + str(endDate) + "' AND time<='" + str(endTime) + "' AND TradeType=0"
        #print(queryMTM)
        return databaseObject.Execute(queryMTM)

    # Function to get total quantity for all short trades
    def getTotalNegQty (self,  portfolioId, feederIndividualId, stockId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryQty = "SELECT SUM(Qty), 1 FROM portfolio_tradesheet_data_table WHERE MetaIndividualId=" + str(portfolioId) + \
                   " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND EntryTime<'" + \
                   str(endTime) + "' AND ExitTime>'" + str(startTime) + "' AND EntryDate='" + str(startDate) + "' AND TradeType=0"
        #print(queryQty)
        return databaseObject.Execute(queryQty)

    # Function to get net MTM for all long trades during training
    def getTrainingTotalPosMTM (self, portfolioId, feederIndividualId, stockId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryMTM = "SELECT SUM(mtm), 1 FROM training_mtm_table WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                   str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND time>'" + str(startTime) + "' AND date>='" + str(startDate) + \
                   "' AND date<='" + str(endDate) + "' AND time<='" + str(endTime) + "' AND TradeType=1"
        #print(queryMTM)
        return databaseObject.Execute(queryMTM)

    # function to get total quantity for all long trades during training
    def getTrainingTotalPosQty (self, portfolioId, feederIndividualId, stockId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryQty = "SELECT SUM(Qty), 1 FROM training_tradesheet_data_table WHERE MetaIndividualId=" + str(portfolioId) + \
                   " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND EntryTime<'" + \
                   str(endTime) + "' AND ExitTime>'" + str(startTime) + "' AND EntryDate='" + str(startDate) + "' AND TradeType=1"
        #print(queryQty)
        return databaseObject.Execute(queryQty)

    # Function to get net MTM for all short trades during training
    def getTrainingTotalNegMTM (self, portfolioId, feederIndividualId, stockId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryMTM = "SELECT SUM(mtm), 1 FROM training_mtm_table WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                   str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND time>'" + str(startTime) + "' AND date>='" + str(startDate) + \
                   "' AND date<='" + str(endDate) + "' AND time<='" + str(endTime) + "' AND TradeType=0"
        #print(queryMTM)
        return databaseObject.Execute(queryMTM)

    # Function to get total quantity for all short trades during training
    def getTrainingTotalNegQty (self, portfolioId, feederIndividualId, stockId, startDate, startTime, endDate, endTime):
        global databaseObject
        queryQty = "SELECT SUM(Qty), 1 FROM training_tradesheet_data_table WHERE MetaIndividualId=" + str(portfolioId) + \
                   " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND EntryTime<'" + \
                   str(endTime) + "' AND ExitTime>'" + str(startTime) + "' AND EntryDate='" + str(startDate) + "' AND TradeType=0"
        #print(queryQty)
        return databaseObject.Execute(queryQty)

    # Function to get Q Matrix of an individual
    def getQMatrix (self, portfolioId, feederIndividualId, stockId):
        global databaseObject
        queryQM = "SELECT row_num, column_num, q_value FROM q_matrix_table WHERE MetaIndividualId=" + str(portfolioId) + \
                   " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId)
        return databaseObject.Execute(queryQM)

    # Function to insert / update Q matrix of an individual
    def updateQMatrix(self, portfolioId, feederIndividualId, stockId, qm):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM q_matrix_table WHERE MetaIndividualId=" + str(portfolioId) + \
                   " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + "), 1"
        resultCheck = databaseObject.Execute(queryCheck)
        for check, dummy in resultCheck:
            if check==1:
                for i in range(0,3,1):
                    for j in range(0,3,1):
                        queryUpdate = "UPDATE q_matrix_table SET q_value=" + str(round(qm[i,j], 10)) + " WHERE MetaIndividualId=" + \
                                      str(portfolioId) + " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + \
                                      str(stockId) + " AND row_num=" + str(i) + " AND column_num=" + str(j)
                        databaseObject.Execute(queryUpdate)
            else:
                for i in range(0,3,1):
                    for j in range(0,3,1):
                        queryInsert = "INSERT INTO q_matrix_table " \
                                     "(MetaIndividualId, IndividualID, SecID, row_num, column_num, q_value)" \
                                     " VALUES " \
                                     "(" + str(portfolioId) + ", " + str(feederIndividualId) + ", " + str(stockId) + ", " + \
                                      str(i) + ", " + str(j) + ", " + str(round(qm[i,j], 10)) + ")"
                        databaseObject.Execute(queryInsert)

    # Function to add individual's entry in reallocation table
    def addNewState(self, portfolioId, feederIndividualId, stockId, date, time, state):
        global databaseObject
        queryNewState = "INSERT INTO reallocation_table" \
                        " (MetaIndividualId, IndividualID, SecID, last_reallocation_date, last_reallocation_time, last_state)" \
                        " VALUES" \
                        " (" + str(portfolioId) + ", " + str(feederIndividualId) + ", " + str(stockId) + ", '" + str(date) + "', '" + \
                        str(time) + "', " + str(state) + ")"
        return databaseObject.Execute(queryNewState)

    # Function to get last state for an individual
    def getLastState (self, portfolioId, feederIndividualId, stockId):
        global databaseObject
        queryLastState = "SELECT last_state, 1 FROM reallocation_table WHERE MetaIndividualId=" + str(portfolioId) + \
                         " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + \
                         " AND last_reallocation_date=(SELECT MAX(last_reallocation_date) FROM reallocation_table WHERE " \
                         "MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + str(feederIndividualId) + \
                         " AND SecID=" + str(stockId) + ") AND last_reallocation_time=(SELECT MAX(last_reallocation_time) " \
                         "FROM reallocation_table WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                         str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND last_reallocation_date=" \
                         "(SELECT MAX(last_reallocation_date) FROM reallocation_table WHERE MetaIndividualId=" + str(portfolioId) + \
                         " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + "))"
        return databaseObject.Execute(queryLastState)

    # Function to get next state for an individual
    def getNextState (self, portfolioId, feederIndividualId, stockId, currentState):
        global databaseObject

        queryMaxQValue = "SELECT MAX(q_value), 1 FROM q_matrix_table WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                         str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND row_num=" + str(currentState)
        resultMaxQValue = databaseObject.Execute(queryMaxQValue)
        queryCurrentQValue = "SELECT q_value, 1 FROM q_matrix_table WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                             str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND row_num=" + str(currentState) + " AND column_num=1"
        resultCurrentQValue = databaseObject.Execute(queryCurrentQValue)
        for maxQValue, dummy1 in resultMaxQValue:
            for currentQValue, dummy2 in resultCurrentQValue:
                # Checking with help of percentage difference between the maximum and current Q value
                if currentQValue!=0:
                    diff = float(abs(maxQValue-currentQValue)/currentQValue*100)
                    if diff>gv.zeroRange:
                        queryNextState = "SELECT column_num, 1 FROM q_matrix_table WHERE MetaIndividualId=" + str(portfolioId) + \
                                         " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND row_num=" + \
                                         str(currentState) + " AND q_value=(SELECT MAX(q_value) FROM q_matrix_table WHERE MetaIndividualId=" + \
                                         str(portfolioId) + " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + \
                                         " AND row_num=" + str(currentState) + ")"
                        return databaseObject.Execute(queryNextState)
                    else:
                        queryNextState = "SELECT column_num, 1 FROM q_matrix_table WHERE MetaIndividualId=" + str(portfolioId) + \
                                         " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND row_num=" + \
                                         str(currentState) + " AND q_value=(SELECT q_value FROM q_matrix_table WHERE MetaIndividualId=" + str(portfolioId) + \
                                         " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + \
                                         " AND row_num=" + str(currentState) + " AND column_num=1)"
                        return databaseObject.Execute(queryNextState)
                else:
                    queryNextState = "SELECT column_num, 1 FROM q_matrix_table WHERE MetaIndividualId=" + str(portfolioId) + \
                                     " AND IndividualID=" + str(feederIndividualId) + " AND SecID=" + str(stockId) + " AND row_num=" + \
                                     str(currentState) + " AND column_num=1"
                    return databaseObject.Execute(queryNextState)

    # Function to reduce free asset for an individual
    def reduceFreeAsset(self, portfolioId, feederIndividualId, stockId, unitQty):
        global databaseObject
        resultCurrentFreeAsset = databaseObject.Execute("SELECT free_asset, total_asset FROM asset_allocation_table "
                                                        "WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" +
                                                        str(feederIndividualId) + " AND SecID=" + str(stockId))
        for freeAsset, totalAsset in resultCurrentFreeAsset:
            if (float(freeAsset)>=unitQty):
                newFreeAsset = float(freeAsset) - unitQty
                newTotalAsset = float(totalAsset) - unitQty
                queryUpdate = "UPDATE asset_allocation_table SET free_asset=" + str(round(newFreeAsset,4)) + ", total_asset=" + \
                              str(round(newTotalAsset,4)) + " WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                              str(feederIndividualId) + " AND SecID=" + str(stockId)
                return databaseObject.Execute(queryUpdate)
            else:
                newTotalAsset = float(totalAsset - freeAsset)
                queryUpdate = "UPDATE asset_allocation_table SET free_asset=0, total_asset=" + str(round(newTotalAsset,4)) + \
                              " WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                              str(feederIndividualId) + " AND SecID=" + str(stockId)
                return databaseObject.Execute(queryUpdate)

    # Function to increase free asset for an individual
    def increaseFreeAsset(self, portfolioId, feederIndividualId, stockId, unitQty):
        global databaseObject
        resultCurrentTotalAsset = databaseObject.Execute("SELECT total_asset, free_asset FROM asset_allocation_table"
                                                        " WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" +
                                                        str(feederIndividualId) + " AND SecID=" + str(stockId))
        for totalAsset, freeAsset in resultCurrentTotalAsset:
            newTotalAsset = float(totalAsset) + unitQty
            newFreeAsset = float(freeAsset) + unitQty
            if newTotalAsset<=gv.maxAsset:
                queryUpdate = "UPDATE asset_allocation_table SET free_asset=" + str(round(newFreeAsset,4)) + ", total_asset=" + \
                              str(round(newTotalAsset,4)) + " WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                              str(feederIndividualId) + " AND SecID=" + str(stockId)
                return databaseObject.Execute(queryUpdate)
            else:
                newTotalAsset = gv.maxAsset
                newFreeAsset = float(freeAsset) + gv.maxAsset - float(totalAsset)
                queryUpdate = "UPDATE asset_allocation_table SET free_asset=" + str(round(newFreeAsset,4)) + ", total_asset=" + \
                              str(round(newTotalAsset,4)) + " WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                              str(feederIndividualId) + " AND SecID=" + str(stockId)
                return databaseObject.Execute(queryUpdate)

    # Function to insert free asset at day end into asset_daily_allocation_table
    def insertDailyAsset(self, portfolioId, date, time):
        global databaseObject
        resultAsset = databaseObject.Execute("SELECT free_asset, 1 from asset_allocation_table where MetaIndividualId=" + str(portfolioId) +
                                             " AND IndividualID=" + str(gv.dummyIndividualId) + " AND SecID=" + str(gv.dummyStockId))
        for totalAsset, dummy in resultAsset:
            databaseObject.Execute("INSERT INTO asset_daily_allocation_table"
                                   " (MetaIndividualId, date, time, total_asset)"
                                   " VALUES"
                                   " (" + str(portfolioId) + ", '" + str(date) + "', '" + str(time) + "', " + str(totalAsset) + ")")

    # Function to delete all non-recent entries from q_matrix_table every walk-forward
    def updateQMatrixTableWalkForward(self, portfolioId):
        global databaseObject
        queryLatest = "SELECT * FROM latest_tblIndividualList WHERE MetaIndividualId=" + str(portfolioId)
        resultLatest = databaseObject.Execute(queryLatest)
        queryUpdate = "DELETE FROM q_matrix_table WHERE MetaIndividualId=" + str(portfolioId) + " AND NOT ( "
        count = 0
        for metaId, feederId, stockId in resultLatest:
            if count==0:
                queryUpdate += "( IndividualID=" + str(feederId) + " AND SecID=" + str(stockId) + " ) "
                count += 1
            else:
                queryUpdate += "OR ( IndividualID=" + str(feederId) + " AND SecID=" + str(stockId) + " ) "
        queryUpdate += ")"
        return databaseObject.Execute(queryUpdate)

    # Function to reset asset_allocation_table every walk-forward
    def updateAssetWalkForward(self, portfolioId):
        global databaseObject
        queryLatest = "SELECT * FROM latest_tblIndividualList WHERE MetaIndividualId=" + str(portfolioId)
        resultLatest = databaseObject.Execute(queryLatest)
        queryUpdate = "DELETE FROM asset_allocation_table WHERE MetaIndividualId=" + str(portfolioId) + " AND NOT ( ( IndividualID=" + \
                      str(gv.dummyIndividualId) + " AND SecID=" + str(gv.dummyStockId) + " ) "
        for metaId, feederId, stockId in resultLatest:
            queryUpdate += "OR ( IndividualID=" + str(feederId) + " AND SecID=" + str(stockId) + " ) "
        queryUpdate += ")"
        return databaseObject.Execute(queryUpdate)

    def resetRanks(self, portfolioId):
        global databaseObject
        queryCount = "SELECT COUNT(*), 1 FROM mapping_table WHERE MetaIndividualId=" + str(portfolioId)
        resultCount = databaseObject.Execute(queryCount)
        for count, dummy in resultCount:
            queryUpdate = "UPDATE ranking_table SET ranking=" + str(count) + " WHERE MetaIndividualId=" + str(portfolioId)
            databaseObject.Execute(queryUpdate)

    # Function to check if given day is a trading day
    def checkTradingDay(self, date):
        global databaseObject
        queryCheck = "SELECT EXISTS (SELECT 1 FROM  tblIndividualTradesheet WHERE EntryDate='" + str(date) + "'), 1"
        return databaseObject.Execute(queryCheck)

    def checkQMatrix(self, portfolioId, feederIndividualId, stockId):
        global databaseObject
        query = "SELECT EXISTS( SELECT 1 FROM latest_tblIndividualList WHERE MetaIndividualId=" + str(portfolioId) + " AND IndividualID=" + \
                str(feederIndividualId) + " AND SecID=" + str(stockId) + " ), 1"
        return databaseObject.Execute(query)