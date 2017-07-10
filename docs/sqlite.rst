SQLite Reference
=====================

To access the commands below, include the following in your script:

.. code-block:: python
   
   import sqlify
   
Converting Text Files (TXT)
----------------------------
.. code-block:: python

   sqlify.text_to_sqlite(file, database, name, delimiter, header=0, na_values=None, skip_lines=None)

Essential Arguments
~~~~~~~~~~~~~~~~~~~~

+-------------+--------------------------------------------------------------------+
| Argument    | Description                                                        |
+-------------+--------------------------------------------------------------------+
| file        | The name of the file to be converted                               |
+-------------+------------------------------------------+-------------------------+
| database    | The name of the database the results should be stored in           |
+-------------+------------------------------------------+-------------------------+
 
Optional Arguments
~~~~~~~~~~~~~~~~~~~

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
| skip_lines  | None                                     | How many of the first n lines of the file to skip                  |
|             |                                          |                                                                    |
|             |                                          | - Works independently of the **header** argument                   |
+-------------+------------------------------------------+--------------------------------------------------------------------+
| na_values   | None                                     | How null values are encoded (default: None)                        |
|             |                                          |                                                                    |
|             |                                          | - Tells SQLify to convert na_values to None before uploading       |
|             |                                          | - Can result in less disk space usage, but also slower uploads     |
+-------------+------------------------------------------+--------------------------------------------------------------------+

Converting Comma-Separated Values (CSV)
----------------------------------------

- This function can also be used to convert other delimiter-seperated values (DSV) as long as the correct delimiter is specified
 
- For example, TSV (tab-separted values)
  
.. code-block:: python
    
   sqlify.csv_to_sqlite(...)
   
- Arguments are the same as for converting text files

Converting SQLite Databases to PostgreSQL
------------------------------------------
Converting a SQLite database is as easy as:

.. code-block:: python
   
   import sqlify
   
   sqlify.sqlite_to_postgres(
       sqlite_db=...,
       pg_db=...,
       name=...,
       host=...,
       username=...,
       password=...
   )
   
The original SQLite table schema will be used for the new PostgreSQL table, and original SQLite data types will be converted according to this conversion table:

+----------------+--------------------+
| SQLite Type    | PostgreSQL Type    |
+================+====================+
| TEXT           | TEXT               |
+----------------+--------------------+
| INTEGER        | BIGINT             |
+----------------+--------------------+
| REAL           | DOUBLE PRECISION   |
+----------------+--------------------+