Creating and Loading `Table` Objects
=====================================

Motivation
-----------
The `Table` is similar in concept to the `DataFrame` (either in R or in pandas) but is optimized for
fast appends, iteration, and copying into databases. Because they are structured as lists of lists,
all of the Python methods for operating with lists apply to Tables. Furthermore, `Table` objects 
provide their own set of specialized methods, with an API inspired by R's `dplyr` package.

The key difference between an R or pandas DataFrame and a pgreaper Table is that the Table is designed
to be copied into a SQL database, rather than attempt to replace its functionality. As such, most of the methods 
are geared towards collecting, cleaning and restructuring data, rather than analyzing it.

Structure
~~~~~~~~~~~
Each list in a Table represents a row, while an item in each row represents a cell. If you had a table stored as--say--`world_gdp_data`, then `world_gdp_data[0]` would return a list representing the first row and `world_gdp_data[0][1]` would be the second column of the first row.

Type-Inference
~~~~~~~~~~~~~~~
`Table` objects keep track of the data types inserted into every column, and uses this information to determine
the final column type. By default, PGReaper will attempt to treat unrecognized data types as `text` columns. Currently,
`Table` is able to recognize text, integer, float, jsonb (list or dict), and datetime types as well as most `numpy` types.


Brief Example: Creating and Loading `Table` Objects
----------------------------------------------------
.. code-block:: python

   from pgreaper import Table, table_to_pg
   
   planes = Table(
    name='planes',
    col_names=['weight', 'length', 'wingspan', 'cost', 'manufacturer']
   )
   
   planes.append(...)
   
   table_to_pg(planes, dbname='vehicles')
   
Full Reference: Creating and Modifying Tables
----------------------------------------------
.. autoclass:: pgreaper.core.Table
   :members: __init__, delete, apply, aggregate, add_col, mutate,
   :private-members:
   
Slicing
~~~~~~~~
.. automethod:: pgreaper.core.Table.__getitem__
   
Exotic Methods
~~~~~~~~~~~~~~~~
These are methods which be handy for some edge cases

.. automethod:: pgreaper.core.Table.as_header
.. automethod:: pgreaper.core.Table.drop_empty
   
Operations Which Create New Tables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. automethod:: pgreaper.core.Table.reorder
.. automethod:: pgreaper.core.Table.subset
.. automethod:: pgreaper.core.Table.transpose

Groupby
~~~~~~~~
.. automethod:: pgreaper.core.Table.groupby
   
Adding Rows to a Table
~~~~~~~~~~~~~~~~~~~~~~~~
Since a `Table` is really just a nested list, you can use the `append()` method.
However, Table objects will refuse to add rows which are shorter or longer than 
the existing rows. This is motivated by the fact that a lot of input sources, 
especially HTML tables, are not very clean and do not guarantee consistent 
record lengths even if they should.

If you want to intentionally extend a table, you can use the `add_col()` method
to create a new column, or `add_dict()` method which implicitly creates new columns.

.. automethod:: pgreaper.core.Table.add_dict

Full Reference: Loading Tables to Postgres
-------------------------------------------

.. autofunction:: pgreaper.copy_table
   
Dumping to Files
-----------------
It is also possible to dump the contents of Table objects into files instead of SQL databases.

.. automodule:: pgreaper.core.table_out

Jupyter Notebooks
------------------
`Table` objects take advantage of Jupyter's pretty HTML display.