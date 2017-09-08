# Generate a CSV with information for 50,000 fake persons

import mimesis
import csv
import random

person = mimesis.Personal(locale='en')

with open('persons.csv', mode='w', newline='\n') as person_file:
    writer = csv.writer(person_file)
    
    # Header Row
    writer.writerow(['Full Name', 'Age', 'Occupation', 'Email', 'Telephone',
        'Nationality'])
    
    for i in range(0, 50000):
        rando = random.uniform(0, 1)
        if rando >= 0.5:
            gender = 'female'
        else:
            gender = 'male'
                
        row = [
            person.full_name(gender=gender),
            person.age(),
            person.occupation(),
            person.email(gender=gender),
            person.telephone(),
            person.nationality()
        ]
        writer.writerow(row)