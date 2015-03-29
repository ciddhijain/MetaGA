__author__ = 'Ciddhi'

from CombinationToLists import *
from Selection import *
from Crossover import *
from Mutation import *
from Convergence import *
from DBUtils import *

if __name__ == "__main__":
    combinationObj = CombinationToLists()
    selectionObj = Selection()
    crossoverObj = Crossover()
    mutationObj = Mutation()
    convergenceObj = Convergence()
    dbObject = DBUtils()
    dbObject.dbConnect()

    dbObject.dbQuery("DELETE FROM mapping_table")

    combinationObj.combine(dbObject)
    generation = 1

    while (True):
        crossoverObj.performCrossover(generation, dbObject, [(1, 2), (2, 2)])
        mutationObj.performMutation(generation, dbObject)
        selectionObj.select(generation, dbObject)
        if (convergenceObj.checkConvergence(generation, dbObject)):
            break
        else:
            generation += 1
    dbObject.dbClose()