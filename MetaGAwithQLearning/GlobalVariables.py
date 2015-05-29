__author__ = 'Ciddhi'

from datetime import timedelta, datetime

databaseName = 'METAGA'                         # This is database name to which connection is made
userName = 'root'                               # This is the user name for database connection
password = 'controljp'                          # This is the password for database connection
dbHost = '127.0.0.1'                            # This is host address for database connection
dbPort = '3306'                                 # This is port for database connection
dbConnector = 'mysqlconnector'                  # This is the connector string to be used, depending upon python package

logFileName = 'MetaGA100.log'                      # This is the name of log file. It will append if file already exists
mappingOutfileName = 'mapping100.csv'
portfolioOutfileName = 'portfolio100.csv'
testingPerformanceOutfileName = 'testingPerformance100.csv'
bestPerformanceOutfileName = 'bestPerformance100.csv'

tradesheetLocation = "TradesheetTable.csv"
priceSeriesLocation = "PriceSeriesTable.csv"
stockTableLocation = "StockTable.csv"
feederIndividualLocation = "FeederIndividualTable.csv"
individualTableLocation = "IndividualTable.csv"

minGenerations = 10                             # This is the minimum number of generations that GA will perform
maxGenerations = 100                            # This is the maximum number of generations that GA will perform

walkforward = 4                                 # This is walkforward from which individual category is taken
#crossoverList = [(1, 2), (2, 2)]               # This list specifies types of crossovers and number of children
# crossoverList = [(1, 2), (2, 2)]

maxPortfolioSize = 100                           # This is the maximum size of portfolio
minPortfolioSize = 25                            # This is the minimum size of portfolio

feederEliteSelectionProbability = 0.6           # This is the probability of putting elite individuals from feeder in a portfolio
feederNonEliteSelectionProbability = 0.4        # This is the probability of putting non-elite fit individuals from feeder in a portfolio
mutationProbability = 0.025                      # This is the mutation probability
longShortProbability = 0.5
longLongProbability = 0.25
shortShortProbability = 0.25

numPortfolios = 100                             # This is the number of portfolios in one generation
maxNumPortfolios = numPortfolios
minNumPortfolios = numPortfolios/2
numCrossoverPortfolios = 0.8 * numPortfolios    # This is the proportion which is generated via crossover
numElites = 5                                   # This is the number of top portfolios in a generation which are considered as elite

startDate = datetime(2012, 1, 2).date()          # This is the start of trading period
endDate = datetime(2012, 2, 29).date()           # This is the end of trading period
testingStartDate = endDate + timedelta(days=1)
testingEndDate = datetime(2012, 3, 31).date()

thresholdPerformance = 0
thresholdMaxPortfolioExposure = 10000000                                       # This is the maximum cap for portfolio exposure
thresholdMinPortfolioExposure = -10000000
factor = 5
thresholdMaxStockExposure = thresholdMaxPortfolioExposure/factor                  # This is the maximum cap for stock exposure in a portfolio
thresholdMinStockExposure = thresholdMinPortfolioExposure/factor

admissiblePerformanceGap = 0.005                 # This takes all portfolios within this range to next generation

dummyPerformance = -50000

newWalkforward = 4
fractionElites = 0.05


#######################################################################################################################################
#Q Learning Varaibles
#######################################################################################################################################

dummyIndividualId = -1
dummyStockId = -1
maxTotalAsset = 10000000
trainingMaxTotalAsset = 20000000
factor = 5
maxAsset = maxTotalAsset / factor
