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
    def insertPortfolioMapping(self, metaIndividualId, feederIndividualId, generation):
        global databaseObject
        query = "INSERT INTO mapping_table" \
                " (meta_individual_id, feeder_individual_id, generation)" \
                " VALUES" \
                " ( " + str(metaIndividualId) + ", " + str(feederIndividualId) + ", " + str(generation) + " )"
        return databaseObject.Execute(query)

    # Function to update a single feeder individual in a portfolio
    def updatePortfolioIndividual(self, metaIndividualId, oldFeederIndividualId, newFeederIndividualId):
        global databaseObject
        query = "UPDATE mapping_table SET feeder_individual_id=" + str(newFeederIndividualId) + \
                " WHERE meta_individual_id=" + str(metaIndividualId) + " AND feeder_individual_id=" + str(oldFeederIndividualId)
        return databaseObject.Execute(query)

    # Function to get portfolio ids in latest generation from mapping_table
    def getPortfolios(self, generation):
        global databaseObject
        query = "SELECT meta_individual_id, COUNT(*) FROM mapping_table WHERE generation=" + \
                str(generation) + " GROUP BY meta_individual_id"
        return databaseObject.Execute(query)


