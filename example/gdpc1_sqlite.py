import sqlify
  
# Load CSV with header on the first row
sqlify.csv_to_sqlite('GDPC1.csv', database='us_gdp.sqlite', name='us_gdp', header=True)