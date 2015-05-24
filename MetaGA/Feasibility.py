__author__ = 'Ciddhi'

import GlobalVariables as gv

class Feasibility:

    def updateFeasibilityByPerformance(self, dbObject):
        dbObject.updatePerformanceFeasibility()
        return

    def updateFeasibilityByPerformancePortfolio(self, portfolioId, dbObject):
        return dbObject.updatePerformanceFeasibilityPortfolio(portfolioId)

    # Delete later
    def updateFeasibilityByExposure(self, generation, dbObject, exposureObject):
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
                    maxStockExposure = exposure

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

    def updateFeasibilityByExposureFromFeederIndividuals(self, generation, dbObject):
        resultPortfolios = dbObject.getPortfolios(generation)
        for portfolioId, size in resultPortfolios:
            resultStocks = dbObject.getPortfolioStocks(portfolioId)
            initialExposure = 0
            maxStockExposure = initialExposure
            for stockId, dummy in resultStocks:
                resultExposureStock = dbObject.getExposurePortfolioStock(portfolioId, stockId)
                for exposure, dummy in resultExposureStock:
                    if exposure > maxStockExposure:
                        maxStockExposure = exposure

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

    def updateFeasibilityByExposurePortfolio(self, portfolioId, dbObject):
        resultStocks = dbObject.getPortfolioStocks(portfolioId)
        initialExposure = 0
        maxStockExposure = initialExposure
        for stockId, dummy in resultStocks:
            resultExposureStock = dbObject.getExposurePortfolioStock(portfolioId, stockId)
            for exposure, dummy in resultExposureStock:
                if exposure > maxStockExposure:
                    maxStockExposure = exposure

        # Calculate portfolio exposure only if stock exposure is within limits
        if maxStockExposure > gv.thresholdStockExposure:
            dbObject.updateExposureFeasibility(portfolioId, 0)          # 0 denotes it is not feasible
            return 0
        else:
            maxPortfolioExposure = initialExposure
            resultPortfolioExposure = dbObject.getExposurePortfolio(portfolioId)
            for exposure, dummy in resultPortfolioExposure:
                if exposure > maxPortfolioExposure:
                    maxPortfolioExposure = exposure
            if maxPortfolioExposure > gv.thresholdPortfolioExposure:
                dbObject.updateExposureFeasibility(portfolioId, 0)
                return 0
            else:
                dbObject.updateExposureFeasibility(portfolioId, 1)
                return 1