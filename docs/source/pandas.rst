Loading pandas DataFrames
===========================

By supporting both INSERTs and UPSERTs for DataFrames, when combined with pandas' `read_sql()` functionality, PGReaper turns any 
Postgres database into a robust store for your pandas-based projects. Compared with other methods for loading DataFrames such as `to_sql()` or a combination of `to_csv()` and `copy_from()`, PGReaper provides:
 * Automatic type inference for basic Python types and most `numpy` types
 * Properly encoding `jsonb` (dict or list) and `timestamp` (datetime) objects
 * Automatic correction of problematic column names, e.g. those that are Postgres keywords
 * Support for composite primary keys
 * Support for both INSERT OR REPLACE and UPSERT operations
    * Faster UPSERT performance using `SELECT unnest()` rather than slower batched INSERTs

.. automodule:: pgreaper.pandas