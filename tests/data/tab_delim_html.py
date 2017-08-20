''' Generate a HTML version of tab_delim.txt '''

import sqlify

sqlify.text_to_html('tab_delim.txt', 'tab_delim.html')
