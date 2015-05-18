__author__ = 'Ciddhi'

from DBUtils import *

if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()

    dbObject.dbQuery("CREATE TABLE stock_table"
                     " ("
                     " stock_id int,"
                     " stock_symbol varchar(20),"
                     " stock_name varchar(50),"
                     " lot_size int"
                     " )")

    dbObject.dbQuery(" CREATE TABLE price_series_table"
                     " ("
                     " stock_id int,"
                     " date date,"
                     " time time,"
                     " open float,"
                     " high float,"
                     " low float,"
                     " close float,"
                     " volume int"
                     " )")

    print("Loading price series table ------ ")

    dbObject.dbQuery("LOAD DATA INFILE '" + gv.priceSeriesLocation + "'"
                     " INTO TABLE price_series_table"
                     " FIELDS TERMINATED BY ','"
                     " ENCLOSED BY '\"'"
                     " LINES TERMINATED BY '\\n'")

    dbObject.dbQuery("CREATE TABLE tradesheet_data_table"
                     " ("
                     " stock_id int,"
                     " individual_id int,"
                     " trade_type int,"
                     " entry_date date,"
                     " entry_time time,"
                     " entry_price float,"
                     " entry_qty int,"
                     " exit_date date,"
                     " exit_time time,"
                     " exit_price float"
                     " )")

    print("Loading tradesheet -----------")

    dbObject.dbQuery("LOAD DATA INFILE '" + gv.tradesheetLocation + "'"
                     " INTO TABLE tradesheet_data_table"
                     " FIELDS TERMINATED BY ','"
                     " ENCLOSED BY '\"'"
                     " LINES TERMINATED BY '\\n'")

    dbObject.dbQuery("DROP TABLE mapping_table")

    dbObject.dbQuery("CREATE TABLE mapping_table"
                     " ("
                     " meta_individual_id int,"
                     " feeder_individual_id int,"
                     " stock_id int"
                     " )")

    dbObject.dbQuery("CREATE TABLE portfolio_table"
                     " ("
                     " meta_individual_id int,"
                     " first_generation int,"
                     " last_generation int,"
                     " feasible_by_performance int DEFAULT NULL,"
                     " feasible_by_exposure int DEFAULT NULL,"
                     " performance float DEFAULT NULL"
                     " )")

    # Comment one of the following.

    dbObject.dbQuery("CREATE TABLE feeder_individual_table"
                     " AS SELECT DISTINCT(individual_id), category, walk_forward, stock_id"
                     " FROM tradesheet_data_table")

    dbObject.dbQuery("CREATE TABLE feeder_individual_table"
                     " ("
                     " walk_forward int,"
                     " stock_id int,"
                     " individual_id int,"
                     " category int"
                     " )")

    dbObject.dbQuery("CREATE TABLE crossover_pairs_table"
                     " ("
                     " meta_individual_id_1 int,"
                     " meta_individual_id_2 int,"
                     " generation int"
                     " )")

    dbObject.dbQuery("CREATE TABLE exposure_table"
                     " ("
                     " individual_id int,"
                     " stock_id int,"
                     " date date,"
                     " time time,"
                     " exposure float")

    dbObject.dbQuery("CREATE TABLE individual_table"
                     " ("
                     " individual_id int,"
                     " stock_id int,"
                     " individual_signature varchar(100),"
                     " individual_type int"
                     " )")

    dbObject.dbClose()