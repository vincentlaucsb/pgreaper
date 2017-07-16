PostgreSQL Reference
======================

Creating New Databases
-----------------------
When trying to upload data into a database that doesn't exist yet, you have 
one of two options:

#. Manually create the database yourself
#. Tell `sqlify` to do it for you

To exercise the second option, continue reading the section below.

Setting the Default Database
------------------------------

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
 
Please be aware that this will store your username and password in a plain-text INI file where sqlify is installed. Once you have modified the default settings, you can review them anytime by typing `sqlify.settings()`. You can also modify one or more settings at a time like:

::

    >>> sqlify.settings(username='vicente')
    [postgres_default]
    username:      vicente
    password:      *************** (Type sqlify.settings(hide=False) to show)
    database:      postgres
    host:          localhost

**Note:** The `host` argument is actually unnecessary in the first example because it defaults to 'localhost'.
    
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

+-------------+------------------------------------------+--------------------------------------------------------------------+
| Argument    | Default                                  | Description                                                        |
+-------------+------------------------------------------+--------------------------------------------------------------------+
| delimiter   | - '\\t' (tab-delimited) for text files   | How individual entries are separated in the data file              |
|             | - ',' for CSV files                      |                                                                    |
+-------------+------------------------------------------+--------------------------------------------------------------------+
| name        | Original filename without extension      | Name of SQL table data should be uploaded to                       |
+-------------+------------------------------------------+--------------------------------------------------------------------+
| header      | 0 (as in, line zero is the header)       | The line number of the header row.                                 |
|             |                                          |                                                                    |
|             |                                          | - header=True is equivalent to header=0                            |
|             |                                          | - No header should be specified with header=False or header=None   |
|             |                                          | - **If header > 0, all lines before header are skipped**           |
+-------------+------------------------------------------+--------------------------------------------------------------------+
| skip_lines  | None                                     | Which of the first n lines of the file to skip                     |
|             |                                          |                                                                    |
|             |                                          | - Works independently of the **header** argument                   |
+-------------+------------------------------------------+--------------------------------------------------------------------+
| null_values | None                                     | A string representing a null value                                 |
+-------------+------------------------------------------+--------------------------------------------------------------------+

