# Generate a JSON with information for 50,000 fake persons

import mimesis
import json
import random

person = mimesis.Personal(locale='en')

with open('persons.json', mode='w', newline='\n', encoding='utf-8') as person_file:
    records = []
    
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
            'Email': person.email(gender=gender),
            'Telephone': person.telephone(),
            'Nationality': person.nationality()
        }
        records.append(row)
        
    person_file.write(json.dumps(records))