__author__ = 'Ciddhi'

from DBUtils import *

if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS tblStockIDList"
                     " ("
                     " SecID int,"
                     " Symbol varchar(20),"
                     " StockName varchar(50),"
                     " LotSize int"
                     " )")

    print("Loading tblStockIDList ------ ")

    dbObject.dbQuery("LOAD DATA INFILE '" + gv.stockTableLocation + "'"
                     " INTO TABLE tblStockIDList"
                     " FIELDS TERMINATED BY ','"
                     " ENCLOSED BY '\"'"
                     " LINES TERMINATED BY '\\n'")

    dbObject.dbQuery(" CREATE TABLE IF NOT EXISTS tblStockPriceData"
                     " ("
                     " SecID int,"
                     " PriceDate date,"
                     " PriceTime time,"
                     " Open float,"
                     " High float,"
                     " Low float,"
                     " Close float,"
                     " Volume int"
                     " )")

    print("Loading tblStockPriceData ------ ")

    dbObject.dbQuery("LOAD DATA INFILE '" + gv.priceSeriesLocation + "'"
                     " INTO TABLE tblStockPriceData"
                     " FIELDS TERMINATED BY ','"
                     " ENCLOSED BY '\"'"
                     " LINES TERMINATED BY '\\n'")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS  tblIndividualTradesheet "
                     " ("
                     " SecID int,"
                     " IndividualID int,"
                     " EntryDate date,"
                     " EntryTime time,"
                     " EntryPrice float,"
                     " ExitDate date,"
                     " ExitTime time,"
                     " ExitPrice float,"
                     " Qty int,"
                     " TradeType int"
                     " )")

    print("Loading tradesheet -----------")

    dbObject.dbQuery("LOAD DATA INFILE '" + gv.tradesheetLocation + "'"
                     " INTO TABLE  tblIndividualTradesheet "
                     " FIELDS TERMINATED BY ','"
                     " ENCLOSED BY '\"'"
                     " LINES TERMINATED BY '\\n'")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS portfolio_tradesheet_data_table"
                     " ("
                     " MetaIndividualId int,"
                     " SecID int,"
                     " IndividualID int,"
                     " EntryDate date,"
                     " EntryTime time,"
                     " EntryPrice float,"
                     " ExitDate date,"
                     " ExitTime time,"
                     " ExitPrice float,"
                     " Qty int,"
                     " TradeType int"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS mapping_table"
                     " ("
                     " MetaIndividualId int,"
                     " IndividualID int,"
                     " SecID int"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS portfolio_table"
                     " ("
                     " MetaIndividualId int,"
                     " first_generation int,"
                     " last_generation int,"
                     " feasible_by_performance int DEFAULT NULL,"
                     " performance float DEFAULT NULL"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS tblIndividualCategoryInfo"
                     " ("
                     " WalkForwardID int,"
                     " SecID int,"
                     " IndividualID int,"
                     " Category int DEFAULT NULL"
                     " )")

    print("Loading tblIndividualCategoryInfo -----------")

    dbObject.dbQuery("LOAD DATA INFILE '" + gv.individualCategoryLocation + "'"
                     " INTO TABLE tblIndividualCategoryInfo"
                     " FIELDS TERMINATED BY ','"
                     " ENCLOSED BY '\"'"
                     " LINES TERMINATED BY '\\n'")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS crossover_pairs_table"
                     " ("
                     " MetaIndividualId_1 int,"
                     " MetaIndividualId_2 int,"
                     " generation int"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS exposure_table"
                     " ("
                     " MetaIndividualId int,"
                     " IndividualID int,"
                     " SecID int,"
                     " date date,"
                     " time time,"
                     " exposure float"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS feeder_performance_table"
                     " ("
                     " IndividualID int,"
                     " SecID int,"
                     " performance float,"
                     " WalkForwardID int"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS tblIndividualList"
                     " ("
                     " IndividualID int,"
                     " SecID int,"
                     " individual_signature varchar(100),"
                     " Type int"
                     " )")

    print("Loading tblIndividualList -----------")

    dbObject.dbQuery("LOAD DATA INFILE '" + gv.individualTableLocation + "'"
                     " INTO TABLE tblIndividualList"
                     " FIELDS TERMINATED BY ','"
                     " ENCLOSED BY '\"'"
                     " LINES TERMINATED BY '\\n'")


    dbObject.dbClose()