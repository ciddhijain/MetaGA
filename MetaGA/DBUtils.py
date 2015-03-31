__author__ = 'Ciddhi'

from DatabaseManager import *
from sqlalchemy import create_engine
import GlobalVariables as gv
from random import randint
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
        query = "SELECT COUNT(*),1 FROM feeder_individual_table WHERE category=3 AND " \
                "walk_forward=" + str(walkforward)
        return databaseObject.Execute(query)

    # Function to return a random elite individual id
    # The provided offset should be within limit depending upon count of elite individuals
    def getRandomEliteIndividual(self, offset, walkforward):
        global databaseObject
        queryIndividual = "SELECT individual_id,1 FROM feeder_individual_table WHERE category=1 AND" \
                          " walk_forward=" + str(walkforward) + " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(queryIndividual)

    # Function to return a random feasible individual id.
    # The provided offset should be within limit depending upon count of feasible individuals
    def getRandomFeasibleIndividual(self, offset, walkforward):
        global databaseObject
        queryIndividual = "SELECT individual_id,1 FROM feeder_individual_table WHERE category=2 AND" \
                          " walk_forward=" + str(walkforward) + " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(queryIndividual)

    # Function to return a random non-feasible individual id
    # The provided offset should be within limit depending upon count of non-feasible individuals
    def getRandomNonFeasibleIndividual(self, offset, walkforward):
        global databaseObject
        queryIndividual = "SELECT individual_id,1 FROM feeder_individual_table WHERE category=3 AND" \
                          " walk_forward=" + str(walkforward) + " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(queryIndividual)

    def getRandomPortfolioIndividual(self, metaIndividualId, offset):
        global databaseObject
        query = "SELECT feeder_individual_id, 1 FROM mapping_table WHERE meta_individual_id=" + \
                str(metaIndividualId) + " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(query)

    # Function to add a new Portfolio mapping in mapping_table
    def insertPortfolioMapping(self, metaIndividualId, feederIndividualId, generation, selected):
        global databaseObject
        query = "INSERT INTO mapping_table" \
                " (meta_individual_id, feeder_individual_id, generation, selected)" \
                " VALUES" \
                " ( " + str(metaIndividualId) + ", " + str(feederIndividualId) + ", " + str(generation) + ", " + str(selected) + " )"
        return databaseObject.Execute(query)

    # Function to insert a new individual in a portfolio with a single change in mapping
    def insertMutationPortfolio(self, metaIndividualId, oldFeederIndividualId, newFeederIndividualId):
        global databaseObject
        queryCurrent = "SELECT feeder_individual_id, generation FROM mapping_table WHERE meta_individual_id=" + str(metaIndividualId)
        resultCurrent = databaseObject.Execute(queryCurrent)
        queryNewMetaId = "SELECT MAX(meta_individual_id), 1 FROM mapping_table"
        resultNewId = databaseObject.Execute(queryNewMetaId)
        for newMetaId, dummy in resultNewId:
            for feederIndividualId, generation in resultCurrent:
                if feederIndividualId==oldFeederIndividualId:
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, generation, selected)" \
                            " VALUES" \
                            " ( " + str(newMetaId+1) + ", " + str(newFeederIndividualId) + ", " + str(generation) + ", " + str(0) + " )"
                    databaseObject.Execute(query)
                else:
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, generation, selected)" \
                            " VALUES" \
                            " ( " + str(newMetaId+1) + ", " + str(feederIndividualId) + ", " + str(generation) + ", " + str(0) + " )"
                    databaseObject.Execute(query)

    # Function to get portfolio ids in given generation from mapping_table
    def getPortfolios(self, generation):
        global databaseObject
        query = "SELECT meta_individual_id, COUNT(*) FROM mapping_table WHERE generation=" + \
                str(generation) + " GROUP BY meta_individual_id"
        return databaseObject.Execute(query)

    # Function to get size of a portfolio in given generation, specified by offset for id
    def getPortfolioSizeByOffset(self, metaPortfolioIdOffset, generation):
        global databaseObject
        queryId = "SELECT DISTINCT(meta_individual_id), 1 FROM mapping_table WHERE generation=" + str(generation) + \
                  " LIMIT 1 OFFSET " + str(metaPortfolioIdOffset)
        resultId = databaseObject.Execute(queryId)
        for metaPortfolioId, dummy in resultId:
            query = "SELECT COUNT(*), meta_individual_id FROM mapping_table WHERE meta_individual_id=" + str(metaPortfolioId)
            return databaseObject.Execute(query)

    # Function to insert children corresponding to crossover
    def insertCrossoverPortfolio(self, crossoverType, numChildren, id1, cut11, cut12, id2, cut21, cut22, generation):
        global databaseObject
        queryCurrent1 = "SELECT feeder_individual_id, 1 FROM mapping_table WHERE meta_individual_id=" + str(id1)
        resultCurrent1 = databaseObject.Execute(queryCurrent1)
        queryCurrent2 = "SELECT feeder_individual_id, 1 FROM mapping_table WHERE meta_individual_id=" + str(id2)
        resultCurrent2 = databaseObject.Execute(queryCurrent2)
        queryNewMetaId = "SELECT MAX(meta_individual_id), 1 FROM mapping_table"
        resultNewId = databaseObject.Execute(queryNewMetaId)

        list1 = []
        list2 = []
        newId = None
        for id, dummy in resultCurrent1:
            list1.append(id)
        for id, dummy in resultCurrent2:
            list2.append(id)
        for id, dummy in resultNewId:
            newId = id

        # Single Point Crossover
        if crossoverType==1:
            for i in range(0, cut11, 1):
                query = "INSERT INTO mapping_table" \
                        " (meta_individual_id, feeder_individual_id, generation, selected)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list1[i]) + ", " + str(generation) + ", " + str(0) + " )"
                databaseObject.Execute(query)
            for i in range(cut21, len(list2), 1):
                query = "INSERT INTO mapping_table" \
                        " (meta_individual_id, feeder_individual_id, generation, selected)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list2[i]) + ", " + str(generation) + ", " + str(0) + " )"
                databaseObject.Execute(query)

            if numChildren==2:
                for i in range(0, cut21, 1):
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, generation, selected)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i]) + ", " + str(generation) + ", " + str(0) + " )"
                    databaseObject.Execute(query)
                for i in range(cut11, len(list1), 1):
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, generation, selected)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list1[i]) + ", " + str(generation) + ", " + str(0) + " )"
                    databaseObject.Execute(query)

            logging.info(str(newId+1) + " and " + str(newId+2) + " generated from type " + str(crossoverType) + "crossover of "
                         + str(id1) + " and " + str(id2) )

        # Two Point Crossover
        if crossoverType==2:
            for i in range(0, cut11, 1):
                query = "INSERT INTO mapping_table" \
                        " (meta_individual_id, feeder_individual_id, generation, selected)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list1[i]) + ", " + str(generation) + ", " + str(0) + " )"
                databaseObject.Execute(query)
            for i in range(cut21, cut22, 1):
                query = "INSERT INTO mapping_table" \
                        " (meta_individual_id, feeder_individual_id, generation, selected)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list2[i]) + ", " + str(generation) + ", " + str(0) + " )"
                databaseObject.Execute(query)
            for i in range(cut12, len(list1), 1):
                 query = "INSERT INTO mapping_table" \
                        " (meta_individual_id, feeder_individual_id, generation, selected)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list1[i]) + ", " + str(generation) + ", " + str(0) + " )"
                 databaseObject.Execute(query)

            if numChildren==2:
                for i in range(0, cut21, 1):
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, generation, selected)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i]) + ", " + str(generation) + ", " + str(0) + " )"
                    databaseObject.Execute(query)
                for i in range(cut11, cut12, 1):
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, generation, selected)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list1[i]) + ", " + str(generation) + ", " + str(0) + " )"
                    databaseObject.Execute(query)
                for i in range(cut22, len(list2), 1):
                     query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, generation, selected)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i]) + ", " + str(generation) + ", " + str(0) + " )"
                     databaseObject.Execute(query)

            logging.info(str(newId+1) + " and " + str(newId+2) + " generated from type " + str(crossoverType) + "crossover of "
                         + str(id1) + " and " + str(id2) )

        #if crossoverType==3:

    # Function to insert a portfolio's performance in performance_table
    def insertPerformance(self, portfolioId, performance):
        global databaseObject
        query = "INSERT INTO performance_table" \
                " (meta_individual_id, performance)" \
                " VALUES" \
                " (" + str(portfolioId) + ", " + str(performance) + ")"
        return databaseObject.Execute(query)

    # Function to get portfolios in a given generation ordered by performance
    def getOrderedPortfolios(self, generation):
        global databaseObject
        query = "SELECT meta_individual_id, performance FROM performance_table WHERE meta_individual_id IN " \
                "(SELECT DISTINCT(meta_individual_id) FROM mapping_table WHERE generation=" + str(generation) + ") " \
                "ORDER BY performance LIMIT " + str(gv.numPortfolios)
        return databaseObject.Execute(query)

    def getOrderedElitePortfolios(self, generation):
        global databaseObject
        query = "SELECT meta_individual_id, performance FROM performance_table WHERE meta_individual_id IN " \
                "(SELECT DISTINCT(meta_individual_id) FROM mapping_table WHERE generation=" + str(generation) + ") " \
                "ORDER BY performance LIMIT " + str(gv.numElites)
        return databaseObject.Execute(query)

    # Function to insert selected portfolios for next generation and update in current generation
    def insertUpdateSelectedPortfolio(self, portfolioId, performance, generation):
        global databaseObject
        queryNewId = "SELECT MAX(meta_individual_id), 1 FROM mapping_table"
        queryFeederIndividuals = "SELECT feeder_individual_id, 1 FROM mapping_table WHERE meta_individual_id=" + str(portfolioId)
        querUpdate = "UPDATE mapping_table SET selected=1 WHERE meta_individual_id=" + str(portfolioId)
        resultNewId = databaseObject.Execute(queryNewId)
        resultFeederIndividuals = databaseObject.Execute(queryFeederIndividuals)
        for newId, dummy1 in resultNewId:
            for feederIndividual, dummy2 in resultFeederIndividuals:
                queryInsert = "INSERT INTO mapping_table" \
                              " (meta_individual_id, feeder_individual_id, generation, selected)" \
                              " VALUES" \
                              " (" + str(newId+1) + ", " + str(feederIndividual) + ", " + str(generation+1) + ", " + str(0) + ")"
                databaseObject.Execute(queryInsert)
            queryPerformance = "INSERT INTO performance_table" \
                               " (meta_individual_id, performance)" \
                               " VALUES" \
                               " (" + str(newId+1) + ", " + str(performance) + ")"
            databaseObject.Execute(queryPerformance)
        return databaseObject.Execute(querUpdate)

    # Function to check if performance for a portfolio has already been calculated
    def checkPerformance(self, portfolioId):
        global databaseObject
        query =  "SELECT EXISTS (SELECT 1 FROM performance_table WHERE meta_individual_id=" + str(portfolioId) + "), 1"
        return databaseObject.Execute(query)

    # Function to get feeder individuals in a portfolio in an ascending order
    def getFeederIndividuals(self, portfolioId):
        global databaseObject
        query = "SELECT DISTINCT(feeder_individual_id), 1 FROM mapping_table WHERE meta_individual_id=" + str(portfolioId) + \
                "ORDER BY feeder_individual_id"
        return databaseObject.Execute(query)