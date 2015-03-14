__author__ = 'Ciddhi'

from random import sample
import GlobalVariables as gv

class Crossover:

    # Function to perform Crossover.
    # By default, it performs single point crossover (type=1).
    # Two point crossover corresponds to 'type=2'.
    # Uniform crossover corresponds to 'type=3'.
    # The variable 'type' takes a list as input and performs all listed types of crossovers.
    # By default, a single point crossover gives two children.
    # The variable numChildren can take a list of values (each belonging to {1, 2}),
    # and gives corresponding number of children for respective type of crossover in list 'type'.
    def performCrossover(self,generation, type=[1], numChildren=[2]):
        groups = sample(range(gv.numPortfolios), gv.numPortfolios)
        
        return None