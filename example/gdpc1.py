import sqlify

# Set default database (only necessary if first time using sqlify)
# sqlify.settings(
    # username='postgres',
    # password='',
    # database='postgres',
    # hostname='localhost')
    
# Load CSV with header on the first row
sqlify.csv_to_pg('GDPC1.csv', database='postgres', name='us_gdp', header=True)