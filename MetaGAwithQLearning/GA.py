__author__ = 'Ciddhi'

from CombinationToLists import *
from Selection import *
from Crossover import *
from Mutation import *
from Convergence import *
from DBUtils import *
from Feasibility import *
from Performance import *
from Exposure import *
import logging
from datetime import datetime
import csv

if __name__ == "__main__":

    logging.basicConfig(filename=gv.logFileName, level=logging.INFO, format='%(asctime)s %(message)s')

    combinationObj = CombinationToLists()
    selectionObj = Selection()
    crossoverObj = Crossover()
    mutationObj = Mutation()
    convergenceObj = Convergence()
    performanceObj = Performance()
    feasibilityObj = Feasibility()
    exposureObj = Exposure()
    dbObject = DBUtils()
    dbObject.dbConnect()

    dbObject.dbQuery("DELETE FROM mapping_table")
    dbObject.dbQuery("DELETE FROM portfolio_table")
    dbObject.dbQuery("DELETE FROM crossover_pairs_table")

    dbObject.dbQuery("DELETE FROM asset_allocation_table")
    dbObject.dbQuery("DELETE FROM asset_daily_allocation_table")
    dbObject.dbQuery("DELETE FROM mtm_table")
    dbObject.dbQuery("DELETE FROM tradesheet_data_table")
    dbObject.dbQuery("DELETE FROM reallocation_table")
    dbObject.dbQuery("DELETE FROM q_matrix_table")
    dbObject.dbQuery("DELETE FROM training_asset_allocation_table")
    dbObject.dbQuery("DELETE FROM training_mtm_table")
    dbObject.dbQuery("DELETE FROM training_tradesheet_data_table")
    dbObject.dbQuery("DELETE FROM ranking_table")

    logging.info("Deleted previous data")

    #exposureObj.calculateExposureIndividuals(dbObject)

    combinationObj.combine(performanceObj, feasibilityObj, dbObject)
    generation = 1

    while (True):
        logging.info("Starting generation " + str(generation))
        print("Starting generation " + str(generation) + " at " + str(datetime.now()))
        crossoverObj.performCrossoverRouletteWheelBiased(generation, performanceObj, feasibilityObj, dbObject)
        mutationObj.performMutation(generation, performanceObj, feasibilityObj, dbObject)
        selectionObj.select(generation, dbObject)
        if (convergenceObj.checkConvergence(generation, dbObject)):
            logging.info("The GA has converged in " + str(generation) + " generations")
            break
        else:
            logging.info("Generation " + str(generation) + " finished")
            logging.info("\n")
            generation += 1

    #dbObject.dbQuery("LOAD DATA INFILE 'E:\\\Studies\\\MTP\\\Results\ 9\ -\ 27th\ April\ 2015\\\performance02.csv' INTO TABLE performance_table FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n'")
    resultElites = dbObject.getFinalElites()
    elites = ()
    for portfolioId, performance in resultElites:
        elites = elites + (portfolioId, )
    performanceElites = performanceObj.calculatePerformancePortfolioList(gv.testingStartDate, gv.testingEndDate, elites, dbObject)
    performanceTradesheet = performanceObj.calculatePerformanceTradesheet(gv.testingStartDate, gv.testingEndDate, dbObject)
    #logging.info("Performance of elites in testing period : " + str(performanceElites[0][1]))
    logging.info("Performance of original tradesheet in testing period : " + str(performanceTradesheet[0][1]))
    print(performanceTradesheet[0][1])
    with open(gv.testingPerformanceOutfileName, 'w') as fp:
        w = csv.writer(fp)
        #w.writerow(["performance elites", "performance original tradesheet"])
        #w.writerow([performanceElites[0][1], performanceTradesheet[0][1]])
        w.writerow(["performance original tradesheet"])
        w.writerow([performanceTradesheet[0][1]])

    generation = 1
    done = True
    with open(gv.bestPerformanceOutfileName, 'w') as fp:
        w = csv.writer(fp)
        w.writerow(["generation", "performance"])
        while(done):
            resultBestPerformance = dbObject.dbQuery("SELECT MAX(performance),1 FROM portfolio_table WHERE last_generation>=" + str(generation) + " AND first_generation<=" + str(generation))
            done = False
            for performance, dummy in resultBestPerformance:
                if performance:
                    done = True
                    w.writerow([generation, performance])
            generation += 1

    dbObject.dbQuery("SELECT * FROM mapping_table"
                     " INTO OUTFILE '" + gv.mappingOutfileName + "'"
                     " FIELDS ENCLOSED BY '\"'"
                     " TERMINATED BY ','"
                     " LINES TERMINATED BY '\\n'")

    dbObject.dbQuery("SELECT * FROM portfolio_table"
                     " INTO OUTFILE '" + gv.portfolioOutfileName + "'"
                     " FIELDS ENCLOSED BY '\"'"
                     " TERMINATED BY ','"
                     " LINES TERMINATED BY '\\n'")

    dbObject.dbClose()