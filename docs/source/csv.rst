Loading TXTs and CSVs to PostgreSQL
=====================================

Details
-------------
Loading Large Files
""""""""""""""""""""
To conserve memory, CSVs are read in chunks. However, memory usage may still be
relatively high in some cases depending on how efficient Python's garbage collection is.

Schema Inference
""""""""""""""""""
Data types are inferred based on the first 7500 lines of a file. Currently, PGReaper
is able to differentiate between text, integer, and floating point numbers.

Ideally, for accurate schema inference, we would want to scan the entire file.
However, this would take unacceptably long for large files. As a result, it is
possible that after the first 7500 rows, a field may contain
a value that is incompatible with the established schema. PGReaper handles 
these rows by storing them in a separate table with the naming scheme
`<original table name>_reject`.

Rows of the Wrong Length
"""""""""""""""""""""""""
PGReaper uses the first row of the file to determine the width of the table. For
99.9% of files, this is not an issue. However, some files may contain malformed rows
which are either shorter or wider than the rest of the file. PGReaper's current
behavior is to drop these rows with a warning (they are not stored in the `_reject` table.

API
----
.. autofunction:: pgreaper.copy_text
.. autofunction:: pgreaper.copy_csv