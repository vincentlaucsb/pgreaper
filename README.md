# SQLify
A Python library for uploading data from various formats (.txt, .csv, .tsv, etc...) to SQLite and PostgreSQL databases.

[![Build Status](https://travis-ci.org/vincentlaucsb/sqlify.svg?branch=master)](https://travis-ci.org/vincentlaucsb/sqlify)
[![Coverage Status](https://coveralls.io/repos/github/vincentlaucsb/sqlify/badge.svg?branch=master)](https://coveralls.io/github/vincentlaucsb/sqlify?branch=master)

## Installation
SQLify can be obtained by cloning this repository, or via the Python package index (https://pypi.org/project/sqlify/1.0.0b1/).

```pip install sqlify```

At a minimum, this package only requires Python 3.4+ and `psycopg2`. SQLify is built on the premise that less dependencies means less trouble.

However, SQLify can also extend the abilities of other amazing Python packages. **Optional** dependencies include:
 * `requests`: For parsing HTML files from the web
 * `pandas`: For INSERT/UPSERT of DataFrames and for returning parsed HTML tables as DataFrames
 * `jupyter-notebook`: For pretty displays of various SQLify objects
 * `sqlalchemy`: For editing live SQL databases

## Features
### Conversion Matrix
SQLify supports converting data from and to these formats:

<table>
<thead>
    <tr>
        <th>From &#8595;/To &#8594;</th>
        <th>TXT</th>
        <th>CSV</th>
        <th>JSON</th>
        <th>HTML Table</th>
        <th>SQLite</th>
        <th>PostgreSQL</th>
        <th>Markdown</th>
    </tr>
</thead>
<tbody>
    <tr>
        <td>TXT</td><td>-</td><td></td><td></td><td></td><td>x</td><td>x</td><td></td>
    </tr>
    <tr>
        <td>CSV</td><td></td><td>-</td><td>x</td><td></td><td>x</td><td>x</td><td></td>
    </tr>
    <tr>
        <td>JSON<sup>1</sup></td><td></td><td></td><td>-</td><td></td><td></td><td>x</td><td></td>
    </tr>
    <tr>
        <td>HTML Table<sup>2</sup></td>
        <td></td><td>x</td><td>x</td><td>-</td><td>x</td><td>x</td><td></td>
    </tr>
    <tr>
        <td>SQLite</td>
        <td></td><td></td><td></td><td></td><td>-</td><td>x</td><td></td>
    </tr>
    <tr>
        <td>PostgreSQL</td>
        <td></td><td>x</td><td></td><td></td><td></td><td>-</td><td></td>
    </tr>
</tbody>
</table>

1. JSON files can be loaded as is or transformed and flattened with the flexible JSON API
1. HTML tables can be gathered and tidied up from files stored locally or through the web (using the `requests` package) using an interactive parser

Furthermore, you can also **load directly from ZIP files** without uncompressing them.

### Transform Your Data, then Bulk Insert or Upsert
In addition to merely performing bulk inserts, you have the luxury of transforming your data as it is being streamed in. Furthermore, for tables that contain a primary key column, you have the option of performing an INSERT OR REPLACE, UPSERT, or INSERT ... ON CONFLICT DO NOTHING.

### Pandas, Jupyter Notebook Integration and More...
SQLify extends the functionality of your favorite Python packages. For example:
 * All tables produced by SQLify, e.g. via the HTML parser, have pretty display methods in Jupyter Notebooks
 * The HTML parser can output either in SQLify's `Table` object or can be converted to a `pandas DataFrame`
 * SQLify can insert or **upsert** pandas DataFrame faster than `pandas`' built-in `to_sql` method

## Philosophy
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

Here's a very quick quick-start guide which demonstrates how to load a CSV containing US real GDP data into a SQL database. You can try this for yourself by downloading the [example subdirectory](/example).

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

## Full Documentation
Documentation can be found [here](http://vincela.com/sqlify).
