# For pgreaper Uploading
from pgreaper.testing import REAL_CSV_DATA, FAKE_CSV_DATA
import pgreaper

# For Clean-Up and Reporting
from memory_profiler import memory_usage
import os
import timeit

# Test 1: US Census 2010
def pgreaper_load():
    pgreaper.copy_csv(
        os.path.join(REAL_CSV_DATA, '2015_StateDepartment.csv'),
        delimiter=',', name='ca_employees', dbname='pgreaper_test',
        header=0)

# Print Results
print("Normal CSV Test")
print(timeit.repeat('pgreaper_load()',
    repeat=1, number=1, globals=globals()))