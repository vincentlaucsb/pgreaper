Loading TXTs and CSVs to PostgreSQL
=====================================

Details
-------------
Loading Large Files
""""""""""""""""""""
To conserve memory, all CSVs are read in chunks.

Schema Inference
""""""""""""""""""
PGReaper uses a custom CSV parser (written in C++) which simultaneously
determines sanitizes and analyzes CSV files. It is capable of differentiating
between strings, integers, and floats. The data type of an entire column is 
determined by this rule:
 * If all values are integers, then the column type is **bigint**
 * If all values are either floats or integers, then the column type is **double precision**
 * Otherwise, the column type is **text**
 
Minor Caveats:
 * Trailing and leading whitespace is ignored when determining data types, so
   * "         3.14          " is considered a floating point number
 * Quoted numeric fields are automatically unquoted (Postgres does not tolerate quoted numeric fields)

Corrections PGReaper Can Make
""""""""""""""""""""""""""""""
Not all CSV files are perfect, but PGReaper is capable of making some corrections:
 * Sanitizing column names
 * Dropping rows that are too short or too long
 * Unquoting numeric fields before copying

API
----
.. autofunction:: pgreaper.copy_text
.. autofunction:: pgreaper.copy_csv