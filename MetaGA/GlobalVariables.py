__author__ = 'Ciddhi'

databaseName = 'METAGA'                            # This is database name to which connection is made
userName = 'root'                               # This is the user name for database connection
password = 'controljp'                          # This is the password for database connection

maxPortfolioSize = 10                           # This is the maximum size of portfolio
minPortfolioSize = 5                            # This is the minimum size of portfolio

feederEliteSelectionProbability = 0.6           # This is the probability of putting elite individuals from feeder in a portfolio
feederNonEliteSelectionProbability = 0.4        # This is the probability of putting non-elite fit individuals from feeder in a portfolio
mutationProbability = 0.005                     # This is the mutation probability

numPortfolios = 10                              # This is the number of portfolios in one generation