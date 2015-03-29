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

    combinationObj.combine(dbObject)
    generation = 1

    while (True):
        crossoverObj.performCrossover(generation, dbObject)
        mutationObj.performMutation(generation, dbObject)
        selectionObj.select(generation, dbObject)
        if (convergenceObj.checkConvergence(generation, dbObject)):
            break
        else:
            generation += 1