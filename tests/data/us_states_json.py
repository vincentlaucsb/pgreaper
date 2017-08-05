''' Generate a JSON version of the US States File '''

import sqlify

sqlify.csv_to_json('us_states.csv', 'us_states.json')