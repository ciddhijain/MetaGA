__author__ = 'Ciddhi'

from datetime import timedelta, datetime

databaseName = 'METAGA'                            # This is database name to which connection is made
userName = 'root'                               # This is the user name for database connection
password = 'controljp'                          # This is the password for database connection
dbHost = '127.0.0.1'
dbPort = '3306'
dbConnector = 'mysqlconnector'
logFileName = 'MetaGA.log'

walkforward = 3
crossoverList = [(1, 2)]
# crossoverList = [(1, 2), (2, 2)]

maxPortfolioSize = 10                           # This is the maximum size of portfolio
minPortfolioSize = 5                            # This is the minimum size of portfolio

feederEliteSelectionProbability = 0.6           # This is the probability of putting elite individuals from feeder in a portfolio
feederNonEliteSelectionProbability = 0.4        # This is the probability of putting non-elite fit individuals from feeder in a portfolio
mutationProbability = 0.05                     # This is the mutation probability

numPortfolios = 10                              # This is the number of portfolios in one generation
numElites = 5                                   # This is the number of top portfolios in a generation which are considered as elite

startDate = datetime(2012, 1, 2).date()         # This is the start of trading period
endDate = datetime(2012, 12, 31).date()           # This is the end of trading period