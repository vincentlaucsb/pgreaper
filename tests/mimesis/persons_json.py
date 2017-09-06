# Generate a CSV with information for 50,000 fake persons

import mimesis
import csv
import random
import json

person = mimesis.Personal(locale='en')
rows = []

for i in range(0, 50000):
    rando = random.uniform(0, 1)
    if rando >= 0.5:
        gender = 'female'
    else:
        gender = 'male'
            
    rows.append({
        'full_name': person.full_name(gender=gender),
        'age': person.age(),
        'occupation': person.occupation(),
        'gender': person.email(gender=gender),
        'telephone': person.telephone(),
        'nationality': person.nationality()
    })
        
with open('persons.json', mode='w') as person_file:
    person_file.write(json.dumps(rows))