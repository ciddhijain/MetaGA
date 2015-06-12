__author__ = 'Ciddhi'

from CombinationToLists import *
from Selection import *
from Crossover import *
from Mutation import *
from Convergence import *
from DBUtils import *
from Performance import *
from Categorization import *
from QLearningWrapper import *
from Reallocation import *
from RewardMatrix import *
from Training import *
from Live import *
from MTM import *
from Ranking import *
from QMatrix import *
from Performance import *
import logging
from datetime import datetime
import GlobalVariables as gv
import csv

if __name__ == "__main__":

    logging.basicConfig(filename=gv.logFileName, level=logging.INFO, format='%(asctime)s %(message)s')

    combinationObj = CombinationToLists()
    selectionObj = Selection()
    crossoverObj = Crossover()
    mutationObj = Mutation()
    convergenceObj = Convergence()
    categoryObj = Categorization()
    qLearningObj = QLearningWrapper()
    rankingObj = Ranking()
    mtmObj = MTM()
    rewardMatrixObj = RewardMatrix()
    qMatrixObj = QMatrix()
    trainingObj = Training()
    liveObj = Live()
    reallocationObj = Reallocation()
    performanceObj = Performance()

    dbObject = DBUtils()
    dbObject.dbConnect()

    dbObject.dbQuery("DELETE FROM mapping_table WHERE MetaIndividualId>50")
    dbObject.dbQuery("DELETE FROM portfolio_table WHERE MetaIndividualId>50")
    dbObject.dbQuery("DELETE FROM crossover_pairs_table")
    dbObject.dbQuery("DELETE FROM portfolio_tradesheet_data_table WHERE MetaIndividualId>50")
    
    dbObject.dbQuery("DELETE FROM asset_allocation_table")
    dbObject.dbQuery("DELETE FROM asset_daily_allocation_table")
    dbObject.dbQuery("DELETE FROM mtm_table")
    dbObject.dbQuery("DELETE FROM reallocation_table")
    dbObject.dbQuery("DELETE FROM q_matrix_table")
    dbObject.dbQuery("DELETE FROM training_asset_allocation_table")
    dbObject.dbQuery("DELETE FROM training_mtm_table")
    dbObject.dbQuery("DELETE FROM training_tradesheet_data_table")
    dbObject.dbQuery("DELETE FROM ranking_table")
    dbObject.dbQuery("DELETE FROM latest_individual_table")
    logging.info("Deleted previous data")

    #categoryObj.categorizeFeederIndividualsByThresholds(gv.startDate, gv.endDate, performanceObj, dbObject)

    #combinationObj.combine(qLearningObj, performanceObj, rankingObj, mtmObj, rewardMatrixObj, qMatrixObj, trainingObj, liveObj, reallocationObj, dbObject)
    generation = 1

    while (True):
        logging.info("Starting generation " + str(generation))
        print("Starting generation " + str(generation) + " at " + str(datetime.now()))
        crossoverObj.performCrossoverRouletteWheelBiased(generation, qLearningObj, performanceObj, rankingObj, mtmObj, rewardMatrixObj, qMatrixObj, trainingObj, liveObj, reallocationObj, dbObject)
        mutationObj.performMutation(generation, qLearningObj, performanceObj, rankingObj, mtmObj, rewardMatrixObj, qMatrixObj, trainingObj, liveObj, reallocationObj, dbObject)
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
        qLearningObj.feedback(portfolioId, qLearningObj, performanceObj, rankingObj, mtmObj, rewardMatrixObj, qMatrixObj, trainingObj, liveObj, reallocationObj, dbObject)
        elites = elites + (portfolioId, )
    performanceElites = performanceObj.calculatePerformancePortfolioList(gv.testingStartDate, gv.testingEndDate, elites, dbObject)
    performanceTradesheet = performanceObj.calculateReferencePerformanceTradesheet(gv.testingStartDate, gv.testingEndDate, dbObject)
    logging.info("Performance of elites in testing period : " + str(performanceElites[0][1]))
    logging.info("Performance of original tradesheet in testing period : " + str(performanceTradesheet[0][1]))
    with open(gv.testingPerformanceOutfileName, 'w') as fp:
        w = csv.writer(fp)
        w.writerow(["performance elites", "performance original tradesheet"])
        w.writerow([performanceElites[0][1], performanceTradesheet[0][1]])
        #w.writerow(["performance original tradesheet"])
        #w.writerow([performanceTradesheet[0][1]])

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