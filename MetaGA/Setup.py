__author__ = 'Ciddhi'

from DBUtils import *

if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()
    '''
    dbObject.dbQuery("CREATE TABLE tradesheet_data_table"
                     " ("
                     " trade_id int,"
                     " individual_id int,"
                     " trade_type int,"
                     " entry_date date,"
                     " entry_time time,"
                     " entry_price float,"
                     " entry_qty int,"
                     " exit_date date,"
                     " exit_time time,"
                     " exit_price float,"
                     " walk_forward int,"
                     " category int"
                     " )")

    '''
    dbObject.dbQuery("DROP TABLE mapping_table")
    dbObject.dbQuery("CREATE TABLE mapping_table"
                     " ("
                     " meta_individual_id int,"
                     " feeder_individual_id int,"
                     " generation int,"
                     " selected int"
                     " )")

    dbObject.dbQuery("CREATE TABLE performance_table"
                     " ("
                     " meta_individual_id int,"
                     " performance float,"
                     " )")

    '''

    dbObject.dbQuery("DROP TABLE feeder_individual_table")

    dbObject.dbQuery("CREATE TABLE feeder_individual_table"
                     " AS SELECT DISTINCT(individual_id), category, walk_forward"
                     " FROM tradesheet_data_table")
    '''

    dbObject.dbClose()