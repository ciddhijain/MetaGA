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

    print("Loading stock table ------ ")

    dbObject.dbQuery("LOAD DATA INFILE '" + gv.stockTableLocation + "'"
                     " INTO TABLE stock_table"
                     " FIELDS TERMINATED BY ','"
                     " ENCLOSED BY '\"'"
                     " LINES TERMINATED BY '\\n'")

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

    dbObject.dbQuery("CREATE TABLE old_tradesheet_data_table"
                     " ("
                     " stock_id int,"
                     " individual_id int,"
                     " entry_date date,"
                     " entry_time time,"
                     " entry_price float,"
                     " exit_date date,"
                     " exit_time time,"
                     " exit_price float,"
                     " entry_qty int,"
                     " trade_type int"
                     " )")

    print("Loading tradesheet -----------")

    dbObject.dbQuery("LOAD DATA INFILE '" + gv.tradesheetLocation + "'"
                     " INTO TABLE old_tradesheet_data_table"
                     " FIELDS TERMINATED BY ','"
                     " ENCLOSED BY '\"'"
                     " LINES TERMINATED BY '\\n'")

    dbObject.dbQuery("CREATE TABLE portfolio_tradesheet_data_table"
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
                     " performance float DEFAULT NULL"
                     " )")

    dbObject.dbQuery("CREATE TABLE individual_category_table"
                     " ("
                     " walk_forward int,"
                     " stock_id int,"
                     " individual_id int,"
                     " category int DEFAULT NULL"
                     " )")

    print("Loading individual_category_table -----------")

    dbObject.dbQuery("LOAD DATA INFILE '" + gv.individualCategoryLocation + "'"
                     " INTO TABLE individual_category_table"
                     " FIELDS TERMINATED BY ','"
                     " ENCLOSED BY '\"'"
                     " LINES TERMINATED BY '\\n'")

    dbObject.dbQuery("CREATE TABLE crossover_pairs_table"
                     " ("
                     " meta_individual_id_1 int,"
                     " meta_individual_id_2 int,"
                     " generation int"
                     " )")

    dbObject.dbQuery("CREATE TABLE exposure_table"
                     " ("
                     " meta_individual_id int,"
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " date date,"
                     " time time,"
                     " exposure float"
                     " )")

    dbObject.dbQuery("CREATE TABLE feeder_performance_table"
                     " ("
                     " feeder_individual_id int,"
                     " stock_id int,"
                     " performance float,"
                     " walk_forward int"
                     " )")

    dbObject.dbQuery("CREATE TABLE individual_table"
                     " ("
                     " individual_id int,"
                     " stock_id int,"
                     " individual_signature varchar(100),"
                     " individual_type int"
                     " )")

    print("Loading individual_table -----------")

    dbObject.dbQuery("LOAD DATA INFILE '" + gv.individualTableLocation + "'"
                     " INTO TABLE individual_table"
                     " FIELDS TERMINATED BY ','"
                     " ENCLOSED BY '\"'"
                     " LINES TERMINATED BY '\\n'")


    dbObject.dbClose()