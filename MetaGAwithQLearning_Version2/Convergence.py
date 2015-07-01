__author__ = 'Ciddhi'

import GlobalVariables as gv
class Convergence:

    def checkConvergence(self, generation, dbObject):
        if generation <= gv.minGenerations:
            return False
        elif generation >= gv.maxGenerations:
            return True
        else:
            resultNew = dbObject.getOrderedElitePortfolios(generation)
            resultOld = dbObject.getOrderedElitePortfolios(generation-1)
            elitesNew = []
            elitesOld = []
            for id, performance in resultNew:
                elitesNew.append((id, performance))
            for id, performance in resultOld:
                elitesOld.append((id, performance))

            # The lists will have same number of elements. So we can use same index
            converged = True

            '''

            for i in range(0, len(elitesOld), 1):

                # If performance of portfolios over same period of time is not same, they are not same
                # This will hold due to the way performance is being calculated
                if elitesOld[i][1]==elitesNew[i][1]:
                    resultFeederNew = dbObject.getFeederIndividualsPortfolio(elitesNew[i][0])
                    resultFeederOld = dbObject.getFeederIndividualsPortfolio(elitesOld[i][0])
                    feederNew = []
                    feederOld = []
                    for stock, id in resultFeederNew:
                        feederNew.append((stock,id))
                    for stock, id in resultFeederOld:
                        feederOld.append((stock,id))

                    feederNew = list(set(feederNew))
                    feederOld = list(set(feederOld))

                    # If number of feeder individuals in each list is not same,
                    # then the portfolios are definitely not the same
                    if len(feederNew)==len(feederOld):

                        # Lastly, we check each individual in portfolio and compare
                        for j in range(0, len(feederOld), 1):
                            if (feederOld[j][0] != feederNew[j][0]) or (feederOld[j][1] != feederNew[j][1]):
                                converged = False
                                break
                        if not converged:
                            break
                    else:
                        converged = False
                        break
                else:
                    converged = False
                    break
            '''

            for i in range(0, len(elitesOld), 1):
                if elitesNew[i][0] != elitesOld[i][0]:
                    converged = False
                    break

            return converged