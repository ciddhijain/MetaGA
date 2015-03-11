__author__ = 'Ciddhi'

from DatabaseManager import *
from sqlalchemy import create_engine
import GlobalVariables as gv
from decimal import Decimal

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

