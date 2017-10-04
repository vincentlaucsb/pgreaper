Loading JSON
=============

From Files
-----------
PGReaper can load arbitrarily large JSON files assuming they are structured
as collections of JSON objects, e.g.

::

    [
        { 
          "Name": "Julia",
          "Age": 29,
          "Occupation": "Database Administrator"
        }, {
          "Name": "Mark",
          "Age": 30,
          "Occupation": "Barista",
          "Phone": 999-999-9999
        }    
    ]

Because it uses a custom JSON reader (which cuts up potentially large files), it is also 
capable of reading other variants of JSON that Python's `json` module can't handle,
such as newline delimited JSON.

Flattening
"""""""""""
Currently, PGReaper can optionally flatten out JSON data by its outermost keys.
In the example above, the resultant table would have two rows with the columns
"name", "age", "occupation", and "phone". The majority of this work is done
via Postgres' JSON functions, which is much faster than anything done in 
pure Python.
    
API
""""
.. automodule:: pgreaper.postgres.json_loader