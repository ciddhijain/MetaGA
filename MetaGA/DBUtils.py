__author__ = 'Ciddhi'

from DatabaseManager import *
from sqlalchemy import create_engine
import GlobalVariables as gv
from random import randint

class DBUtils:

    databaseObject = None

    def dbConnect (self):
        db_username = gv.userName
        db_password = gv.password
        db_host = '127.0.0.1'
        db_name = gv.databaseName
        db_port = '3306'
        global databaseObject
        databaseObject = DatabaseManager(db_username, db_password,db_host,db_port, db_name)
        databaseObject.Connect()

    def dbQuery (self, query):
        global databaseObject
        return databaseObject.Execute(query)

    def dbClose (self):
        global databaseObject
        databaseObject.Close()

    # Function to return the count of Elite individuals in table feeder_individual_table
    def getEliteCountLatest(self):
        global databaseObject
        query = "SELECT COUNT(*),1 FROM feeder_individual_table WHERE category=1 AND " \
                "walk_forward=(SELECT MAX(walk_forward) FROM feeder_individual_table)"
        return databaseObject.Execute(query)

    # Function to return the count of Feasible individuals in table feeder_individual_table
    def getFeasibleCountLatest(self):
        global databaseObject
        query = "SELECT COUNT(*),1 FROM feeder_individual_table WHERE category=2 AND " \
                "walk_forward=(SELECT MAX(walk_forward) FROM feeder_individual_table)"
        return databaseObject.Execute(query)

    # Function to return the count of Non-Feasible individuals in table feeder_individual_table
    def getNonFeasibleCountLatest(self):
        global databaseObject
        query = "SELECT COUNT(*),1 FROM feeder_individual_table WHERE category=3 AND " \
                "walk_forward=(SELECT MAX(walk_forward) FROM feeder_individual_table)"
        return databaseObject.Execute(query)

    # Function to return a random elite individual id
    # The provided offset should be within limit depending upon count of elite individuals
    def getRandomEliteIndividualLatest(self, offset):
        global databaseObject
        queryIndividual = "SELECT individual_id,1 FROM feeder_individual_table WHERE category=1 AND" \
                          " walk_forward=(SELECT MAX(walk_forward) FROM feeder_individual_table)" \
                          " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(queryIndividual)

    # Function to return a random feasible individual id.
    # The provided offset should be within limit depending upon count of feasible individuals
    def getRandomFeasibleIndividualLatest(self, offset):
        global databaseObject
        queryIndividual = "SELECT individual_id,1 FROM feeder_individual_table WHERE category=2 AND" \
                          " walk_forward=(SELECT MAX(walk_forward) FROM feeder_individual_table)" \
                          " LIMIT 1 OFFSET " + str(offset)
        return databaseObject.Execute(queryIndividual)

    # Function to return a random non-feasible individual id
    # The provided offset should be within limit depending upon count of non-feasible individuals
    def getRandomNonFeasibleIndividualLatest(self, offset):
        global databaseObject
        queryIndividual = "SELECT individual_id,1 FROM feeder_individual_table WHERE category=3 AND" \
                          " walk_forward=(SELECT MAX(walk_forward) FROM feeder_individual_table)" \
                          " LIMIT 1 OFFSET " + str(offset)
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
                            " ( " + str(newMetaId+1) + ", " + str(newFeederIndividualId) + ", " + str(generation) + ", " + str(1) + " )"
                    databaseObject.Execute(query)
                else:
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, generation, selected)" \
                            " VALUES" \
                            " ( " + str(newMetaId+1) + ", " + str(feederIndividualId) + ", " + str(generation) + ", " + str(1) + " )"
                    databaseObject.Execute(query)

    # Function to get portfolio ids in latest generation from mapping_table
    def getPortfolios(self, generation):
        global databaseObject
        query = "SELECT meta_individual_id, COUNT(*) FROM mapping_table WHERE generation=" + \
                str(generation) + " GROUP BY meta_individual_id"
        return databaseObject.Execute(query)

    # Function to get size of a portfolio in given generation, specified by offset for id
    def getPortfolioSizeByOffset(self, metaPortfolioIdOffset, generation):
        global databaseObject
        queryId = "SELECT DISTINCT(meta_portfolio_id), 1 FROM mapping_table WHERE generation=" + str(generation) + " LIMIT 1 OFFSET " + str(metaPortfolioIdOffset)
        resultId = databaseObject.Execute(queryId)
        for metaPortfolioId, dummy in resultId:
            query = "SELECT COUNT(*), meta_portfolio_id FROM mapping_table WHERE meta_portfolio_id=" + str(metaPortfolioId)
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
                        " ( " + str(newId+1) + ", " + str(list1[i]) + ", " + str(generation) + ", " + str(1) + " )"
                databaseObject.Execute(query)
            for i in range(cut21, len(list2), 1):
                query = "INSERT INTO mapping_table" \
                        " (meta_individual_id, feeder_individual_id, generation, selected)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list2[i]) + ", " + str(generation) + ", " + str(1) + " )"
                databaseObject.Execute(query)

            if numChildren==2:
                for i in range(0, cut21, 1):
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, generation, selected)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i]) + ", " + str(generation) + ", " + str(1) + " )"
                    databaseObject.Execute(query)
                for i in range(cut11, len(list1), 1):
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, generation, selected)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list1[i]) + ", " + str(generation) + ", " + str(1) + " )"
                    databaseObject.Execute(query)

        # Two Point Crossover
        if crossoverType==2:
            for i in range(0, cut11, 1):
                query = "INSERT INTO mapping_table" \
                        " (meta_individual_id, feeder_individual_id, generation, selected)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list1[i]) + ", " + str(generation) + ", " + str(1) + " )"
                databaseObject.Execute(query)
            for i in range(cut21, cut22, 1):
                query = "INSERT INTO mapping_table" \
                        " (meta_individual_id, feeder_individual_id, generation, selected)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list2[i]) + ", " + str(generation) + ", " + str(1) + " )"
                databaseObject.Execute(query)
            for i in range(cut12, len(list1), 1):
                 query = "INSERT INTO mapping_table" \
                        " (meta_individual_id, feeder_individual_id, generation, selected)" \
                        " VALUES" \
                        " ( " + str(newId+1) + ", " + str(list1[i]) + ", " + str(generation) + ", " + str(1) + " )"
                 databaseObject.Execute(query)

            if numChildren==2:
                for i in range(0, cut21, 1):
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, generation, selected)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i]) + ", " + str(generation) + ", " + str(1) + " )"
                    databaseObject.Execute(query)
                for i in range(cut11, cut12, 1):
                    query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, generation, selected)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list1[i]) + ", " + str(generation) + ", " + str(1) + " )"
                    databaseObject.Execute(query)
                for i in range(cut22, len(list2), 1):
                     query = "INSERT INTO mapping_table" \
                            " (meta_individual_id, feeder_individual_id, generation, selected)" \
                            " VALUES" \
                            " ( " + str(newId+2) + ", " + str(list2[i]) + ", " + str(generation) + ", " + str(1) + " )"
                     databaseObject.Execute(query)

        #if crossoverType==3:
