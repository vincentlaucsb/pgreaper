'''
Generate a newline-delimited JSON (NDJSON)
with information for 50,000 fake persons

Ref: http://specs.okfnlabs.org/ndjson/
Notes:
 - Default encoding for JSON is UTF-8
 - '\n' and '\r\n' are both valid line terminators
'''

import mimesis
import json
import random

person = mimesis.Personal(locale='en')

with open('persons.ndjson', mode='w', newline='\n', encoding='utf-8') as person_file:
    for i in range(0, 50000):
        rando = random.uniform(0, 1)
        if rando >= 0.5:
            gender = 'female'
        else:
            gender = 'male'
                
        row = {
            'Full Name': person.full_name(gender=gender),
            'Age': person.age(),
            'Occupation': person.occupation(),
            'Email': person.email(),
            'Telephone': person.telephone(),
            'Nationality': person.nationality()
        }
        
        person_file.write(json.dumps(row))
        person_file.write('\n')