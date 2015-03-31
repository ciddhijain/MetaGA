__author__ = 'Ciddhi'

class Convergence:

    def checkConvergence(self, generation, dbObject):
        if generation == 1:
            return False
        elif generation == 100:
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

            for i in range(0, len(elitesOld), 1):

                # If performance of portfolios over same period of time is not same, they are not same
                # This will hold due to the way performance is being calculated
                if elitesOld[i][1]==elitesNew[i][1]:
                    resultFeederNew = dbObject.getFeederIndividuals(elitesNew[i][0])
                    resultFeederOld = dbObject.getFeederIndividuals(elitesOld[i][0])
                    feederNew = []
                    feederOld = []
                    for id, dummy in resultFeederNew:
                        feederNew.append(id)
                    for id, dummy in resultFeederOld:
                        feederOld.append(id)

                    # If number of feeder individuals in each list is not same,
                    # then the portfolios are definitely not the same
                    if len(feederNew)==len(feederOld):

                        # Lastly, we check each individual in portfolio and compare
                        for j in range(0, len(feederOld), 1):
                            if feederOld[j] != feederNew[j]:
                                converged= False
                                break
                        if not converged:
                            break
                    else:
                        converged = False
                        break
                else:
                    converged = False
                    break

            return converged