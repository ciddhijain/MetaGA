__author__ = 'Ciddhi'

from DBUtils import *
import GlobalVariables as gv

class Reallocation:

    def reallocate(self, portfolioId, startDate, startTime, endDate, endTime, dbObject):

        # get all individuals which are active in last window
        resultIndividuals = dbObject.getIndividuals(portfolioId, startDate, startTime, endDate, endTime)
        posDeltaIndividuals = []
        negDeltaIndividuals = []
        noDeltaIndividuals = []

        for individualId, stockId in resultIndividuals:
            # get last state for the individual
            resultLastState = dbObject.getLastState(portfolioId, individualId, stockId)
            for lastState, dummy1 in resultLastState:
                resultNextState = dbObject.getNextState(portfolioId, individualId, stockId, lastState)
                for nextState, dummy2 in resultNextState:
                    # Depending upon suggested next state, segregate individual_id
                    if nextState==0:
                        negDeltaIndividuals.append((portfolioId, individualId, stockId))
                    else:
                        if nextState==1:
                            noDeltaIndividuals.append((portfolioId, individualId, stockId))
                        else:
                            posDeltaIndividuals.append((portfolioId, individualId, stockId))

        # update asset and state for all individuals accordingly
        for i in range(0, len(negDeltaIndividuals), 1):
            dbObject.reduceFreeAsset(portfolioId, negDeltaIndividuals[i][1], negDeltaIndividuals[i][2], gv.unitQty)
            dbObject.addNewState(portfolioId, negDeltaIndividuals[i][1], negDeltaIndividuals[i][2], endDate, endTime, 0)
        for i in range(0, len(posDeltaIndividuals), 1):
            dbObject.increaseFreeAsset(portfolioId, posDeltaIndividuals[i][1], posDeltaIndividuals[i][2], gv.unitQty)
            dbObject.addNewState(portfolioId, posDeltaIndividuals[i][1], posDeltaIndividuals[i][2], endDate, endTime, 2)
        for i in range(0, len(noDeltaIndividuals), 1):
            dbObject.addNewState(portfolioId, noDeltaIndividuals[i][1], noDeltaIndividuals[i][2], endDate, endTime, 1)


# To test
if __name__ == "__main__":
    dbObject = DBUtils()
    dbObject.dbConnect()
    reallocationObject = Reallocation()
    reallocationObject.reallocate(dbObject)
    dbObject.dbClose()




