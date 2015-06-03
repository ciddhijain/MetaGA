__author__ = 'Ciddhi'

from datetime import timedelta, datetime

databaseName = 'QLGA1'                         # This is database name to which connection is made
userName = 'root'                               # This is the user name for database connection
password = 'controljp'                          # This is the password for database connection
dbHost = '127.0.0.1'                            # This is host address for database connection
dbPort = '3306'                                 # This is port for database connection
dbConnector = 'mysqlconnector'                  # This is the connector string to be used, depending upon python package

logFileName = 'MetaGAFeedback200.log'                      # This is the name of log file. It will append if file already exists
mappingOutfileName = 'mapping200.csv'
portfolioOutfileName = 'portfolio200.csv'
testingPerformanceOutfileName = 'testingPerformance200.csv'
bestPerformanceOutfileName = 'bestPerformance200.csv'

tradesheetLocation = "TradesheetTable.csv"
priceSeriesLocation = "PriceSeriesTable.csv"
stockTableLocation = "StockTable.csv"
individualCategoryLocation = "categories.csv"
individualTableLocation = "IndividualTable.csv"

minGenerations = 10                             # This is the minimum number of generations that GA will perform
maxGenerations = 100                            # This is the maximum number of generations that GA will perform

walkforward = 4                                 # This is walkforward from which individual category is taken
startDate = datetime(2012, 1, 2).date()          # This is the start of trading period
endDate = datetime(2012, 3, 31).date()           # This is the end of trading period
testingStartDate = endDate + timedelta(days=1)
testingEndDate = datetime(2012, 6, 30).date()
numTrainingDays = 62

maxPortfolioSize = 50                           # This is the maximum size of portfolio
minPortfolioSize = 25                            # This is the minimum size of portfolio

feederEliteSelectionProbability = 0.6           # This is the probability of putting elite individuals from feeder in a portfolio
feederNonEliteSelectionProbability = 0.4        # This is the probability of putting non-elite fit individuals from feeder in a portfolio
mutationProbability = 0.025                      # This is the mutation probability
longShortProbability = 0.5
longLongProbability = 0.25
shortShortProbability = 0.25

numPortfolios = 50                             # This is the number of portfolios in one generation
maxNumPortfolios = numPortfolios
minNumPortfolios = numPortfolios/2
numCrossoverPortfolios = 0.8 * numPortfolios    # This is the proportion which is generated via crossover
numElites = 5                                 # This is the number of top portfolios in a generation which are considered as elite

thresholdPerformance = 0

admissiblePerformanceGap = 0.005                 # This takes all portfolios within this range to next generation

dummyPerformance = -50000

fractionElites = 0.05

#######################################################################################################################################
#Q Learning Varaibles
#######################################################################################################################################

dummyIndividualId = -1
dummyStockId = -1

maxTotalAsset = 5000000
trainingMaxTotalAsset = 2 * maxTotalAsset
factor = 5
maxAsset = maxTotalAsset / factor
unitQty = 250000

alpha = 0.6
gamma = 0.8
maxGreedyLevel = 5
zeroRange = 0.001

hourWindow = 1