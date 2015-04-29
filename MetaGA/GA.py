__author__ = 'Ciddhi'

from CombinationToLists import *
from Selection import *
from Crossover import *
from Mutation import *
from Convergence import *
from DBUtils import *
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
    dbObject = DBUtils()
    dbObject.dbConnect()

    dbObject.dbQuery("DELETE FROM mapping_table")
    dbObject.dbQuery("DELETE FROM performance_table")
    logging.info("Deleted previous data")


    combinationObj.combine(dbObject)
    generation = 1

    while (True):
        logging.info("Starting generation " + str(generation))
        print("Starting generation " + str(generation) + " at " + str(datetime.now()))
        crossoverObj.performCrossover(generation, dbObject, gv.crossoverList)
        mutationObj.performMutation(generation, dbObject)
        selectionObj.select(generation, performanceObj, dbObject)
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


    dbObject.dbQuery("SELECT * FROM mapping_table"
                     " INTO OUTFILE '" + gv.mappingOutfileName + "'"
                     " FIELDS ENCLOSED BY '\"'"
                     " TERMINATED BY ','"
                     " LINES TERMINATED BY '\\n'")

    dbObject.dbQuery("SELECT DISTINCT(p.meta_individual_id), p.performance, m.generation"
                     " FROM mapping_table m, performance_table p"
                     " WHERE m.meta_individual_id=p.meta_individual_id"
                     " INTO OUTFILE '" + gv.performanceOutfileName + "'"
                     " FIELDS ENCLOSED BY '\"'"
                     " TERMINATED BY ','"
                     " LINES TERMINATED BY '\\n'")

    dbObject.dbClose()