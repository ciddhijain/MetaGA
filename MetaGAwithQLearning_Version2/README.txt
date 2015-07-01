This is the MetaGA with Q Learning incorporated for each portfolio. It does ranking for for the rankingDays (specified in GlobalVariables.py), initializes q matrix for initializationDays (specified in GlobalVariables.py) and does live trading for liveDays (specified in GlobalVariables.py).

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