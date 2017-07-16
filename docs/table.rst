Data Structures
=================

(To be completed)

When SQLify uploads files into SQLite and PostgreSQL databases, it first reads them into `Table` and `PgTable` objects respectively. However, you can also create and modify these objects yourself.

Because `PgTable` is a subclass of `Table`, the usage and methods of `Table` objects will be explained first.

''Actual Example:'' https://github.com/vincentlaucsb/UC-Employee-Salaries/blob/master/Loading%20JSON%20Data%20into%20SQL.ipynb

`Table` Objects
----------------
`Table` objects are simply lists of lists. Each list in a Table represents a row, while an item in each "row" represents a cell. If you had a table stored as--say--`world_gdp_data`, then `world_gdp_data[0]` would return a list representing the first row and `world_gdp_data[0][1]` would be the second column of the first row.

Creating and Loading `Table` Objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

   from sqlify import Table, table_to_sqlite
   
   data = Table(...)
   
   table_to_sqlite(data)

Jupyter Notebooks
~~~~~~~~~~~~~~~~~~
`Table` objects take advantage of Jupyter's pretty HTML display.
   
Possible Issues
~~~~~~~~~~~~~~~~
While custom `Table` objects should load succesfully if they were constructed reasonably, real-word data isn't always reasonable and may lead to ...

`PgTable` Objects
------------------
There are only two real differences between `PgTable` objects and `Table` objects. The first is that the `guess_type()` method of PgTable objects returns, as you would expect, PostgreSQL data types instead of SQLite data types. Secondly, `PgTable` also defines a `read()` method which returns one row of data from the table as a tab-delimited string. This makes `PgTable` compatible with `psycopg2`'s `copy` methods.

Loading a `PgTable` into Postgres
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The easiest way to load a custom-made `PgTable` is with the `table_to_pg()` function.

.. code-block::python

   import sqlify

   lotsa_data = sqlify.PgTable(...)

   # Some more Python code

   table_to_pg(lotsa_data, database='postgres')

Using `PgTable` with `psycopg2`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
However, for more complicated use cases, you may wish to access psycopg2's `copy_from()` or `copy_expert()` functions directly.

.. code-block:: python

   import psycopg2
   import sqlify

   lotsa_data = sqlify.PgTable(...)
   
   with psycopg2.connect(dbname=..., user=..., host=..., password=...) as conn:
       cur = conn.cursor()
       cur.copy_from(lotsa_data, 'one_large_table', sep='\t')