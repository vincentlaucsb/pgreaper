# For pgreaper Uploading
from pgreaper.testing import REAL_CSV_DATA, FAKE_CSV_DATA
import pgreaper
import psycopg2

# For Clean-Up and Reporting
from memory_profiler import memory_usage
import os
import timeit
import matplotlib.pyplot as plt

# 2015 California State Employees Salaries
def pgreaper_load():
    pgreaper.copy_csv(
        os.path.join(REAL_CSV_DATA, '2015_StateDepartment.csv'),
        delimiter=',', name='ca_employees', dbname='pgreaper_test',
        header=0)      
        
# Clean Up
def clean_up():
    # Drop test database
    with psycopg2.connect(**pgreaper.PG_DEFAULTS) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn.cursor().execute('DROP DATABASE pgreaper_test')
        
# Print Results
print("CSV Upload Test")
print(timeit.repeat('pgreaper_load()', repeat=1, number=1, globals=globals()))
clean_up()