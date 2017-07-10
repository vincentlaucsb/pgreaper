# SQLify
A Python library for uploading data from various formats (.txt, .csv, .tsv, etc...) to SQLite and PostgreSQL databases.

## Installation
SQLify can be obtained by cloning this repository, or via the Python package index (https://pypi.org/project/sqlify/1.0.0b1/).

```pip install sqlify```

The only dependency is `psycopg2` for interfacing with PostgreSQL databases.

## Features
### Easy To Use
SQLify is built around the premise that you have better things to do than 
learning how to use a complicated library to perform the mundane chore of 
uploading data into a SQL database. As such, it only asks of you the bare 
minimum amount of information to get the job done. Tedious tasks such as 
getting column names and types are automated.

### Fast and Efficient
SQLify won't blow up your computer's RAM by trying to load an entire 2 GB file
at once. Furthermore, there's very little overhead between your files and the
SQL database. For SQLite, records are inserted via mass insert statements, 
while for Postgres, the fast COPY protocol is used. This allows tens of
thousands of records to be inserted in only seconds.

### Robust
SQLify is designed to handle common defects in data files. Column
names are checked for duplicates and offensive characters.

For Postgres, a separate table is automatically created for rejects which don't
fit with the specified schema. Thus, SQLify is faster than 
using bulk insert statements but more flexible than a vanilla COPY statement.

## Example: Loading US GDP Data into Postgres
*Data Source: https://fred.stlouisfed.org/series/GDPC1*

Here's a very quick quick-start guide which demonstrates how to load a CSV of real US GDP data into a SQL database.

```python
import sqlify

# Set default database (only necessary if first time using sqlify)
sqlify.settings(
    username='postgres',
    password='',
    database='postgres',
    hostname='localhost')
    
# Load CSV with header on the first row
sqlify.csv_to_pg('GDPC1.csv', database='postgres', name='us_gdp', header=True)
```

### SQLite
Similarly, loading this CSV into a SQLite database can be performed with the one-liner
```python
sqlify.csv_to_sqlite('GDPC1.csv', database='us_gdp.sqlite', name='us_gdp', header=True)
```
## Capabilities
* Upload delimiter seperated files (e.g. CSV) into SQL databases
* Convert SQLite databases to PostgreSQL
* Create and modify table-like data structures in Python and upload them to SQL

## Full Documentation
Documentation can be found [here](http://vincela.com/sqlify).