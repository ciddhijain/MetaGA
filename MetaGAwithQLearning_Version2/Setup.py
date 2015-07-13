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

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS tblIndividualList"
                     " ("
                     " IndividualID int,"
                     " SecID int,"
                     " individual_signature varchar(100),"
                     " Type int"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS tblWalkForwardList"
                     " ("
                     " WalkForwardID int,"
                     " TrainingBeginDate date,"
                     " TrainingEndDate date,"
                     " ReportingBeginDate date DEFAULT NULL,"
                     " ReportingEndDate date DEFAULT NULL"
                     ")")


    dbObject.dbClose()