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

    #dbObject.dbQuery("DELETE FROM mapping_table WHERE meta_individual_id>12")
    #dbObject.dbQuery("DELETE FROM portfolio_table WHERE meta_individual_id>12")
    #dbObject.dbQuery("DELETE FROM crossover_pairs_table")
    dbObject.dbQuery("UPDATE portfolio_table SET last_generation=1")
    logging.info("Deleted previous data")

    #exposureObj.calculateExposureIndividuals(dbObject)

    #combinationObj.combine(performanceObj, feasibilityObj, dbObject)
    generation = 1

    while (True):
        logging.info("Starting generation " + str(generation))
        print("Starting generation " + str(generation) + " at " + str(datetime.now()))
        #crossoverObj.performCrossoverRouletteWheelBiased(generation, performanceObj, feasibilityObj, dbObject)
        #mutationObj.performMutation(generation, performanceObj, feasibilityObj, dbObject)
        selectionObj.select(generation, dbObject)
        if (convergenceObj.checkConvergence(generation, dbObject)):
            logging.info("The GA has converged in " + str(generation) + " generations")
            break
        else:
            logging.info("Generation " + str(generation) + " finished")
            logging.info("\n")
            generation += 1

    #dbObject.dbQuery("LOAD DATA INFILE 'E:\\\Studies\\\MTP\\\Results\ 9\ -\ 27th\ April\ 2015\\\performance02.csv' INTO TABLE performance_table FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n'")
    '''
    resultElites = dbObject.getFinalElites()
    elites = ()
    for portfolioId, dummy in resultElites:
        elites = elites + (portfolioId, )
    performanceElites = performanceObj.calculatePerformancePortfolioList(gv.testingStartDate, gv.testingEndDate, elites, dbObject)
    performanceTradesheet = performanceObj.calculatePerformanceTradesheet(gv.testingStartDate, gv.testingEndDate, dbObject)
    logging.info("Performance of elites in testing period : " + str(performanceElites[0][1]))
    logging.info("Performance of original tradesheet in testing period : " + str(performanceTradesheet[0][1]))
    with open(gv.testingPerformanceOutfileName, 'w') as fp:
        w = csv.writer(fp)
        w.writerow(["performance elites", "performance original tradesheet"])
        w.writerow([performanceElites[0][1], performanceTradesheet[0][1]])
    '''


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