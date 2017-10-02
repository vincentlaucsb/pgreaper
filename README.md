# pgreaper
[![Build Status](https://travis-ci.org/vincentlaucsb/pgreaper.svg?branch=master)](https://travis-ci.org/vincentlaucsb/pgreaper)
[![Coverage Status](https://coveralls.io/repos/github/vincentlaucsb/pgreaper/badge.svg?branch=master)](https://coveralls.io/github/vincentlaucsb/pgreaper?branch=master)

PGReaper is the easy Pythonic way to upload data from CSV, JSON, HTML, and
SQLite sources to PostgreSQL databases. Interally, it uses the fast `COPY`
streaming protocol, but wraps it in a way that makes it more flexible, robust and
easier to use. Features include:
 * Automatic schema inference
 * Ability to parse and normalize `<table>`s in HTML
 * Ability to flatten and/or extract nested keys from JSON before copying
 * Ability to copy files in `.zip` archives without decompressing them
 * Ability to copy over networks
 
Lastly, but certainly not least, PGReaper ships with its own `Table` data structure
while also being able to copy `pandas` DataFrames. These allows programmatic creation
or updating of SQL tables without the verbosity associated with traditional ORMs.
Furthermore, when uploading Python data structures, PGReaper automatically infers the schema,
including for `JSONB` (from dict or list) and `timestamp` (from datetime) objects.

## Benchmarks
Speed is one of `pgreaper`'s main design goals. A list of benchmarks may be found under the `benchmarks` subdirectory.

## Installation
I've been using PGReaper heavily for my own projects, such as Twitter and web scraping, but I have only recently started polishing up the documentation and API for public consumption. PGReaper will be released on PyPI when I feel it is mature enough, but if you would like to use it now, you can clone this repository and run the following command where the files are extracted.

```pip install .```

Currently, you may need Cython to build this project.

### Dependencies
`PGReaper` requires a minimum of dependencies. Namely, these are Python 3.5+, `psycopg2`, and obviously PostgreSQL (use 9.3+ for JSON features).

## Documentation
Full documentation (or at least that's the goal!) is [available here](http://vincela.com/pgreaper/).
As a user of software, I frequently get frustrated with inconsistent, inaccurate
 or unclear documentation. However, as a writer of software I also realize
 writing documentation isn't always fun and what is obvious to you isn't always 
 obvious to others. If you see something that needs
 improvement, feel free to submit an issue or pull request.