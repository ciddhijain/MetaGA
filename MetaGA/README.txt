Database Structure :
- tblindividuallist corresponds to individual_table
- tblindividualtradesheet corresponds to old_tradesheet_data_table
- tblstockidlist corresponds to stock_table
- tblstockpricedata corresponds to price_series_table

Steps for setup:
- Create a Database and update its name in GlobalVariables.py. Also update username, password, and connection string etc.
- Load the above mentioned tables in the database
- individual_category_table is to contain category information. Update variables newWalkforward and percentageElites in Global Variables and Run Categorization.py
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


