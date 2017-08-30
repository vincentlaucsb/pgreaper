# pgreaper
[![Build Status](https://travis-ci.org/vincentlaucsb/pgreaper.svg?branch=master)](https://travis-ci.org/vincentlaucsb/pgreaper)
[![Coverage Status](https://coveralls.io/repos/github/vincentlaucsb/pgreaper/badge.svg?branch=master)](https://coveralls.io/github/vincentlaucsb/pgreaper?branch=master)

PGReaper is the easy Pythonic way to upload data from CSV, JSON, HTML, and
SQLite sources to PostgreSQL databases. Interally, it uses the fast `COPY`
streaming protocol, but wraps it in a way that makes it more robust and
easier to use. Features include:
 * Automatic schema inference
 * Automatic error handling
 * Ability to parse and normalize `<table>`s in HTML
 * Ability to flatten and/or extract nested keys from JSON before copying
 * Ability to copy files in `.zip` archives without decompressing them
 * Ability to copy over networks
 
Lastly, but certainly not least, PGReaper ships with its own `Table` data structure
while also being able to copy `pandas` DataFrames. These allows programmatic creation
or updating of SQL tables without the verbosity associated with traditional ORMs.
Furthermore, when uploading Python data structures, PGReaper automatically infers the schema,
including for `JSONB` (from dict or list) and `timestamp` (from datetime) objects.

## Installation
PGReaper is currently under heavy development, but will be released on PyPI when I feel it is mature enough. If you would like to use it, you can clone this repository and run the following command where the files are extracted.

```pip install .```

### Dependencies
`PGReaper` requires a minimum of dependencies. Namely, these are Python 3.5+, `psycopg2`, and obviously PostgreSQL (use 9.3+ for JSON features).
