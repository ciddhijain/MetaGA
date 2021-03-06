This is the basic MetaGA. It generates tradesheet by thresholding exposure for a portfolio, overall and stock-wise.

Steps for setup:
- Create a Database and update its name in GlobalVariables.py. Also update username, password, and connection string etc.
- Update variables startDate and endDate in Global Variables (these correspond to start and end of training period) and Run Categorization.py
- Set other variables in Global Variables:
    - numPortfolios i.e. initial population size
    - walkforward
    - minPortfolioSize and maxPortfolioSize
    - mutationProbability
    - longLongProbability, longShortProbability, shortShortProbability
    - feederEliteSelectionProbability and feederNonEliteSelectionProbability
    - numCrossoverPortfolios i.e. proportion generated via crossover
    - minGenerations and maxGenerations
    - thresholdMaxPortfolioExposure and thresholdMaxStockExposure
    - thresholdMinPortfolioExposure and thresholdMinStockExposure
- Run GA.py
- Export portfolio_table