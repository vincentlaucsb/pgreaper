import sqlify

sqlify.csv_to_pg('us_states.csv', database='sqlify_pg_test', name='us_states', delimiter=',', header=0)