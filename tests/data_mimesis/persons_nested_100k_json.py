import mimesis
import csv
import random
import json
import gzip

person = mimesis.Personal(locale='en')
rows = []

for i in range(0, 100000):
    rando = random.uniform(0, 1)
    if rando >= 0.5:
        gender = 'female'
    else:
        gender = 'male'
            
    rows.append({
        'full_name': person.full_name(gender=gender),
        'nationality': person.nationality(),
        'occupation': person.occupation(),
        'personal': {
            'age': person.age(),
            'gender': gender,
        },
        'contact': {        
            'telephone': {
                'mobile': person.telephone(),
                'work': person.telephone()
            },
            'email': person.email(gender=gender)
        }
    })
        
with gzip.open('persons_nested_100k.json.gz', mode='wb') as person_file:
    person_file.write(bytes(json.dumps(rows), encoding='utf-8'))