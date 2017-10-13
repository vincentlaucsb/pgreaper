# Generate a JSON with information for 500,000 fake persons
import mimesis
import json
import random

# For pgreaper Uploading
import pgreaper
import psycopg2

# For Clean-Up and Reporting
from memory_profiler import memory_usage
import os
import timeit
import matplotlib.pyplot as plt

# Generate JSON
person = mimesis.Personal(locale='en')

with open('persons.json', mode='w', newline='\n', encoding='utf-8') as person_file:
    records = []
    
    for i in range(0, 500000):
        rando = random.uniform(0, 1)
        if rando >= 0.5:
            gender = 'female'
        else:
            gender = 'male'
                
        row = {
            'Full Name': person.full_name(gender=gender),
            'Age': person.age(),
            'Occupation': person.occupation(),
            'Email': person.email(gender=gender),
            'Telephone': person.telephone(),
            'Nationality': person.nationality()
        }
        records.append(row)
        
    person_file.write(json.dumps(records))

# Load JSON Data
def pgreaper_load():
    pgreaper.copy_json('persons.json', flatten='outer',
        delimiter=',', dbname='pgreaper_test', header=0)      
        
# Clean Up
def clean_up():
    # Drop test database
    with psycopg2.connect(**pgreaper.PG_DEFAULTS) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn.cursor().execute('DROP DATABASE pgreaper_test')
        
# Print Results
print("JSON Upload Test")
print(timeit.repeat('pgreaper_load()', repeat=1, number=1, globals=globals()))
clean_up()