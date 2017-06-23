# SQLify
A Python library for uploading data from various formats (.txt, .csv, .tsv, etc...) to SQLite and PostgreSQL databases.

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

## Full Documentation
Documentation can be found [here](http://vincela.com/sqlify).
