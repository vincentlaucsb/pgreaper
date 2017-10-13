pgreaper Uploading Benchmarks
==============================

## CSV Files

[2015_StateDepartment.csv](https://github.com/vincentlaucsb/csv-data/blob/58e5e4f7100737820f649ef600ff4fdf1a880fbb/real_data/2015_StateDepartment.csv)
 * 71.8 MB
 * Contains quoting in all fields--including numeric ones (which Postgres does not like)
   * pgreaper's CSV parser automatically unquotes fields if they are not needed
 * 7.6 seconds
 
## JSON Files

See json_benchmark.py
 * About 80MB
 * 10.7 seconds (to flatten outermost keys)