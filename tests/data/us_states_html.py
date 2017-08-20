''' Generate a HTML version of the US States File '''

import sqlify

sqlify.csv_to_html('us_states.csv', 'us_states.html')