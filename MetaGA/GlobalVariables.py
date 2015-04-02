__author__ = 'Ciddhi'

from datetime import timedelta, datetime

databaseName = 'METAGA2'                         # This is database name to which connection is made
userName = 'root'                               # This is the user name for database connection
password = 'controljp'                          # This is the password for database connection
dbHost = '127.0.0.1'                            # This is host address for database connection
dbPort = '3306'                                 # This is port for database connection
dbConnector = 'mysqlconnector'                  # This is the connector string to be used, depending upon python package

logFileName = 'MetaGA02.log'                      # This is the name of log file. It will append if file already exists
mappingOutfileName = 'mapping02.csv'
performanceOutfileName = 'performance02.csv'
bestPerformanceOutfileName = 'bestPerformance02.csv'

tradesheetLocation = "IndividualInfo0.csv"
priceSeriesLocation = "IndividualInfo_Ver3.1.1_AXISBANK1.csv"

maxGenerations = 100                            # This is the maximum number of generations that GA will perform
walkforward = 2                                 # This is walkforward from which individuals and trades will be picked
crossoverList = [(1, 2), (2, 2)]                        # This list specifies types of crossovers and number of children
# crossoverList = [(1, 2), (2, 2)]

maxPortfolioSize = 40                           # This is the maximum size of portfolio
minPortfolioSize = 25                            # This is the minimum size of portfolio

feederEliteSelectionProbability = 0.6           # This is the probability of putting elite individuals from feeder in a portfolio
feederNonEliteSelectionProbability = 0.4        # This is the probability of putting non-elite fit individuals from feeder in a portfolio
mutationProbability = 0.10005                      # This is the mutation probability

numPortfolios = 200                             # This is the number of portfolios in one generation
numElites = 50                                   # This is the number of top portfolios in a generation which are considered as elite

startDate = datetime(2012, 1, 2).date()         # This is the start of trading period
endDate = datetime(2012, 6, 30).date()         # This is the end of trading period