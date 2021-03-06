__author__ = 'Ciddhi'

from datetime import timedelta, datetime

databaseName = 'METAGA1'                         # This is database name to which connection is made
userName = 'root'                               # This is the user name for database connection
password = 'controljp'                          # This is the password for database connection
dbHost = '127.0.0.1'                            # This is host address for database connection
dbPort = '3306'                                 # This is port for database connection
dbConnector = 'mysqlconnector'                  # This is the connector string to be used, depending upon python package

logFileName = 'MetaGA101.log'                      # This is the name of log file. It will append if file already exists
mappingOutfileName = 'mapping101.csv'
portfolioOutfileName = 'portfolio101.csv'
testingPerformanceOutfileName = 'testingPerformance101.csv'
bestPerformanceOutfileName = 'bestPerformance101.csv'

minGenerations = 10                             # This is the minimum number of generations that GA will perform
maxGenerations = 100                            # This is the maximum number of generations that GA will perform

walkforward = 1                                 # This is walkforward from which individual category is taken
startDate = datetime(2012, 1, 2).date()          # This is the start of training period
endDate = datetime(2012, 1, 31).date()           # This is the end of training period
testingStartDate = endDate + timedelta(days=1)
testingEndDate = datetime(2012, 2, 15).date()
numTrainingDays = 20

maxPortfolioSize = 75                           # This is the maximum size of portfolio
minPortfolioSize = 25                            # This is the minimum size of portfolio

feederEliteSelectionProbability = 0.7           # This is the probability of putting elite individuals from feeder in a portfolio
feederNonEliteSelectionProbability = 0.3        # This is the probability of putting non-elite fit individuals from feeder in a portfolio
mutationProbability = 0.05                      # This is the mutation probability
longShortProbability = 0.5
longLongProbability = 0.25
shortShortProbability = 0.25

numPortfolios = 50                             # This is the number of portfolios in one generation
maxNumPortfolios = numPortfolios
minNumPortfolios = numPortfolios/2
numCrossoverPortfolios = 0.8 * numPortfolios    # This is the proportion which is generated via crossover
numElites = 5                                   # This is the number of top portfolios in a generation which are considered as elite

thresholdPerformance = 0
thresholdMaxPortfolioExposure = 10000000                                       # This is the maximum cap for portfolio exposure
thresholdMinPortfolioExposure = -10000000
factor = 5
thresholdMaxStockExposure = thresholdMaxPortfolioExposure/factor                  # This is the maximum cap for stock exposure in a portfolio
thresholdMinStockExposure = thresholdMinPortfolioExposure/factor

admissiblePerformanceGap = 0.005                 # This takes all portfolios within this range to next generation

dummyPerformance = -50000

fractionElites = 0.05
