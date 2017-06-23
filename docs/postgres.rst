PostgreSQL Reference
======================

Creating New Databases
-----------------------
When trying to upload data into a database that doesn't exist yet, you have 
one of two options:

#. Manually create the database yourself
#. Tell `sqlify` to do it for you

To exercise the second option, continue reading the section below.

Saving Connection Settings
---------------------------
If most of your work is done using the same combinations of username,
password, and host, you can save these settings instead of re-entering them.

Setting the Default Database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to create new databases, a connection is first made to the default database. 
From there, a `CREATE DATABASE` command is issued. This is no different from 
creating databases via the command line (using psql, see:
https://www.postgresql.org/docs/current/static/manage-ag-createdb.html).

An example of how to set the default database is shown below:

.. code-block:: python

   import sqlify
   sqlify.settings(database='postgres',
                   username='postgres',
                   password='',
                   hostname='localhost')
 
Please be aware that this will store your username and password in a plain-text INI file where sqlify is installed.

**Note:** The `host` argument is actually unnecessary here because it defaults to 'localhost'.

Converting Text and CSV Files
------------------------------
The functions below use similar arguments to their SQLite equivalents, but 
have extra arguments corresponding to either database connections or 
additional Postgres features.

.. code-block:: python
    
   sqlify.text_to_pg(...)   
   sqlify.csv_to_pg(...)
   
Essential Arguments
~~~~~~~~~~~~~~~~~~~~~~~~~

* file: The name of the file to be uploaded
* database: The name of the destination database

Connection Arguments
~~~~~~~~~~~~~~~~~~~~~~~~~~

* database
* username
* password
* host

All of these except for database are optional. If not specified, the default database settings are used. For example, if you want to create a new database on the same account and host as the default database, then you only need to specify the "database" argument.

Optional Arguments
~~~~~~~~~~~~~~~~~~~~~~~~

* delimiter: Defaults to '\t' for text files and ',' for CSV files
* header: Zero-indexed line number of the row containing the header, i.e. 0
  specifies the first row as header. Defaults to 0.
  
  - Should be set to **False** if there is no header
  - **Note**: All lines before the header row are always ignored 
  
* skip_lines: Which of the first n lines of the file to skip. Defaults to None.

  - Works independently of the **header** argument
  
* null_values: A string representing a null value

Type-Guessing
~~~~~~~~~~~~~~~

Columns types are inferred based on the data type of the entries in the first 10,000 lines of the file. When uploading to Postgres, the data type for a column is based on the type that accomodates all entries in that column. For example, if a column has 9999 integers and 1 string, then the column type is set to "text".

Rejected Rows
--------------
Because data is not perfect, there may be a few records which do not mesh
with the specified schema. Because Postgres is strongly-typed, these records
must either be discarded to stored elsewhere. SQLify handles this issue by 
storing rejected rows in a table where the columns have the same name as the 
original, but all types are set to "text". The name of this table is original 
table name followed by "_reject". For example, if a table is named "us_census"
then the table of rejected records is called "us_census_reject".