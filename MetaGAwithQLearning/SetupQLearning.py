__author__ = 'Ciddhi'

from DBUtilsQLearning import *

if __name__ == "__main__":

    dbObject = DBUtilsQLearning()
    dbObject.dbConnect()

    dbObject.dbQuery("CREATE TABLE latest_individual_table"
                     " ("
                     " meta_individual_id,"
                     " feeder_individual_id int,"
                     " stock_id int"
                     " )")

    dbObject.dbQuery("CREATE TABLE training_mtm_table"
                     " ("
                     " meta_individual_id int,"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " trade_type int,"
                     " date date,"
                     " time time,"
                     " mtm float"
                     " )")

    dbObject.dbQuery("CREATE TABLE training_tradesheet_data_table"
                     " ("
                     " meta_individual_id int"
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

    dbObject.dbQuery("CREATE TABLE training_asset_allocation_table"
                     " ("
                     " meta_individual_id int"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " total_asset decimal(15,4),"
                     " used_asset decimal(15,4),"
                     " free_asset decimal(15,4)"
                     " )")

    dbObject.dbQuery("CREATE TABLE ranking_table"
                     " ("
                     " meta_individual_id int"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " ranking int"
                     " )")

    dbObject.dbQuery("CREATE TABLE asset_daily_allocation_table"
                     "("
                     " meta_individual_id int"
                     " date date,"
                     " time time,"
                     " total_asset decimal(15,4)"
                     ")")

    dbObject.dbQuery("CREATE TABLE tradesheet_data_table"
                     " ("
                     " meta_individual_id int"
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

    dbObject.dbQuery("CREATE TABLE mtm_table"
                     " ("
                     " meta_individual_id int,"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " trade_type int,"
                     " date date,"
                     " time time,"
                     " mtm float"
                     " )")

    dbObject.dbQuery("CREATE TABLE asset_allocation_table"
                     " ("
                     " meta_individual_id int"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " total_asset decimal(15,4),"
                     " used_asset decimal(15,4),"
                     " free_asset decimal(15,4)"
                     " )")

    dbObject.dbQuery("CREATE TABLE q_matrix_table"
                     " ("
                     " meta_individual_id int"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " row_num int,"
                     " column_num int,"
                     " q_value decimal(20,10)"
                     " )")

    dbObject.dbQuery("CREATE TABLE reallocation_table"
                     " ("
                     " meta_individual_id int"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " last_reallocation_date date,"
                     " last_reallocation_time time,"
                     " last_state int"
                     " )")

    dbObject.dbClose()
