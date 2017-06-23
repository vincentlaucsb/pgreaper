'''
Create a SQLite database of randomly generated numbers
(Adapted from test_sqlite.py)

Write the text file that will be converted to SQL
 * Tab-delimited
 * 500 rows + a row of headers
 * 5 columns
 
Columns are:
 1. Random samples from a Uniform(0, 1) distribution
 2. Random samples from a Uniform(0, 10) distribution
 3. Random samples from a standard Normal distribution
 4. Random samples from a Normal(5, 25) distribution
 5. Random samples from an Exponential(1) distribution
'''

import sqlify

import random
import statistics
import os
import unittest
import sqlite3

random.seed(1)

header = 'UNIFORM0_1\tUNIFORM0_10\tSTDNORM\tNORM5_25\tEXP_1\n'

# Create columns
uniform0_1 = [random.uniform(0, 1) for i in range(0, 500)]
uniform0_10 = [random.uniform(0, 10) for i in range(0, 500)]
stdnorm = [random.normalvariate(0, 1) for i in range(0, 500)]
norm5_25 = [random.normalvariate(5, 25) for i in range(0, 500)]
exp_1 = [random.expovariate(1) for i in range(0, 500)]

with open('sqlite_numbers.txt', 'w') as test_file:
    test_file.write(header)
    
    for i in range(0, 500):
        test_file.write('{0}\t{1}\t{2}\t{3}\t{4}\n'.format(
            uniform0_1[i], uniform0_10[i], stdnorm[i], norm5_25[i],
            exp_1[i]))
            
# Create the database
sqlify.text_to_sql(
    'sqlite_numbers.txt',
    database='sqlite_numbers.db',
    col_types='REAL',
    name="random_numbers",
    delimiter='\t')