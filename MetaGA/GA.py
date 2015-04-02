__author__ = 'Ciddhi'

from CombinationToLists import *
from Selection import *
from Crossover import *
from Mutation import *
from Convergence import *
from DBUtils import *
import logging
from datetime import datetime

if __name__ == "__main__":

    logging.basicConfig(filename=gv.logFileName, level=logging.INFO, format='%(asctime)s %(message)s')

    combinationObj = CombinationToLists()
    selectionObj = Selection()
    crossoverObj = Crossover()
    mutationObj = Mutation()
    convergenceObj = Convergence()
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
        selectionObj.select(generation, dbObject)
        if (convergenceObj.checkConvergence(generation, dbObject)):
            logging.info("The GA has converged in " + str(generation) + " generations")
            break
        else:
            logging.info("Generation " + str(generation) + " finished")
            logging.info("\n")
            generation += 1

    dbObject.dbQuery("SELECT * FROM mapping_table"
                     " INTO OUTFILE " + gv.mappingOutfileName +
                     " FIELDS ENCLOSED BY '\"'"
                     " TERMINATED BY ','"
                     " LINES TERMINATED BY '\\n'")

    dbObject.dbQuery("SELECT * FROM performance_table"
                     " INTO OUTFILE " + gv.performanceOutfileName +
                     " FIELDS ENCLOSED BY '\"'"
                     " TERMINATED BY ','"
                     " LINES TERMINATED BY '\\n'")

    dbObject.dbClose()