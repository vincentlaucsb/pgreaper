''' Generate a Markdown version of the US States File '''

import sqlify

sqlify.csv_to_md('us_states.csv', 'us_states.md')