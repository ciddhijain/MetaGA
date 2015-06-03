__author__ = 'Ciddhi'

from DBUtils import *

if __name__ == "__main__":

    dbObject = DBUtils()
    dbObject.dbConnect()

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS latest_individual_table"
                     " ("
                     " MetaIndividualId int,"
                     " IndividualID int,"
                     " SecID int"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS training_mtm_table"
                     " ("
                     " MetaIndividualId int,"
                     " IndividualID int,"
                     " SecID int,"
                     " TradeType int,"
                     " date date,"
                     " time time,"
                     " mtm float"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS training_tradesheet_data_table"
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

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS training_asset_allocation_table"
                     " ("
                     " MetaIndividualId int,"
                     " IndividualID int,"
                     " SecID int,"
                     " total_asset decimal(15,4),"
                     " used_asset decimal(15,4),"
                     " free_asset decimal(15,4)"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS ranking_table"
                     " ("
                     " MetaIndividualId int,"
                     " IndividualID int,"
                     " SecID int,"
                     " ranking int"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS asset_daily_allocation_table"
                     "("
                     " MetaIndividualId int,"
                     " date date,"
                     " time time,"
                     " total_asset decimal(15,4)"
                     ")")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS mtm_table"
                     " ("
                     " MetaIndividualId int,"
                     " IndividualID int,"
                     " SecID int,"
                     " TradeType int,"
                     " date date,"
                     " time time,"
                     " mtm float"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS asset_allocation_table"
                     " ("
                     " MetaIndividualId int,"
                     " IndividualID int,"
                     " SecID int,"
                     " total_asset decimal(15,4),"
                     " used_asset decimal(15,4),"
                     " free_asset decimal(15,4)"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS q_matrix_table"
                     " ("
                     " MetaIndividualId int,"
                     " IndividualID int,"
                     " SecID int,"
                     " row_num int,"
                     " column_num int,"
                     " q_value decimal(20,10)"
                     " )")

    dbObject.dbQuery("CREATE TABLE IF NOT EXISTS reallocation_table"
                     " ("
                     " MetaIndividualId int,"
                     " IndividualID int,"
                     " SecID int,"
                     " last_reallocation_date date,"
                     " last_reallocation_time time,"
                     " last_state int"
                     " )")

    dbObject.dbClose()
