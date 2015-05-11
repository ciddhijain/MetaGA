__author__ = 'Ciddhi'

import GlobalVariables as gv

class Feasibility:

    def updateFeasibilityByPerformance(self, dbObject):
        dbObject.updatePerformanceFeasibility()
        return

    def updateFeasibiltyByExposure(self, generation, dbObject, exposureObject):
        resultPortfolios = dbObject.getPortfolios(generation)
        for portfolioId, size in resultPortfolios:
            resultStocks = dbObject.getPortfolioStocks(portfolioId)
            for stockId, dummy in resultStocks:
                exposureObject.calculateExposurePortfolioStock(portfolioId, stockId, dbObject)
            resultMaxStockExposure = dbObject.getMaxExposurePortfolioStock(portfolioId)
            initialExposure = 0
            maxStockExposure = initialExposure
            for exposure, stockId in resultMaxStockExposure:
                if exposure > maxStockExposure:
                    maxExposure = exposure

            # Calculate portfolio exposure only if stock exposure is within limits
            if maxStockExposure > gv.thresholdStockExposure:
                dbObject.updateExposureFeasibility(portfolioId, 0)          # 0 denotes it is not feasible
            else:
                maxPortfolioExposure = initialExposure
                resultPortfolioExposure = dbObject.getExposurePortfolio(portfolioId)
                for exposure, dummy in resultPortfolioExposure:
                    if exposure > maxPortfolioExposure:
                        maxPortfolioExposure = exposure
                if maxPortfolioExposure > gv.thresholdPortfolioExposure:
                    dbObject.updateExposureFeasibility(portfolioId, 0)
                else:
                    dbObject.updateExposureFeasibility(portfolioId, 1)
        return