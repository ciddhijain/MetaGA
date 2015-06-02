__author__ = 'Ciddhi'

from DBUtils import *

if __name__ == "__main__":

    dbObject = DBUtils()
    dbObject.dbConnect()

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS latest_individual_table"
                     " ("
                     " meta_individual_id int,"
                     " feeder_individual_id int,"
                     " stock_id int"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS training_mtm_table"
                     " ("
                     " meta_individual_id int,"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " trade_type int,"
                     " date date,"
                     " time time,"
                     " mtm float"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS training_tradesheet_data_table"
                     " ("
                     " meta_individual_id int,"
                     " stock_id int,"
                     " feeder_individual_id int,"
                     " entry_date date,"
                     " entry_time time,"
                     " entry_price float,"
                     " exit_date date,"
                     " exit_time time,"
                     " exit_price float,"
                     " entry_qty int,"
                     " trade_type int"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS training_asset_allocation_table"
                     " ("
                     " meta_individual_id int,"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " total_asset decimal(15,4),"
                     " used_asset decimal(15,4),"
                     " free_asset decimal(15,4)"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS ranking_table"
                     " ("
                     " meta_individual_id int,"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " ranking int"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS asset_daily_allocation_table"
                     "("
                     " meta_individual_id int,"
                     " date date,"
                     " time time,"
                     " total_asset decimal(15,4)"
                     ")")

    # Following table is created in other Setup.py
    '''
    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS portfolio_tradesheet_data_table"
                     " ("
                     " meta_individual_id int,"
                     " stock_id int,"
                     " feeder_individual_id int,"
                     " entry_date date,"
                     " entry_time time,"
                     " entry_price float,"
                     " exit_date date,"
                     " exit_time time,"
                     " exit_price float,"
                     " entry_qty int,"
                     " trade_type int"
                     " )")
    '''

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS mtm_table"
                     " ("
                     " meta_individual_id int,"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " trade_type int,"
                     " date date,"
                     " time time,"
                     " mtm float"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS asset_allocation_table"
                     " ("
                     " meta_individual_id int,"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " total_asset decimal(15,4),"
                     " used_asset decimal(15,4),"
                     " free_asset decimal(15,4)"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS q_matrix_table"
                     " ("
                     " meta_individual_id int,"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " row_num int,"
                     " column_num int,"
                     " q_value decimal(20,10)"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS reallocation_table"
                     " ("
                     " meta_individual_id int,"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " last_reallocation_date date,"
                     " last_reallocation_time time,"
                     " last_state int"
                     " )")

    dbObject.dbClose()
