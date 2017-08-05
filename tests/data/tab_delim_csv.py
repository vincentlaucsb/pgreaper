''' Generate a CSV version of tab_delim.txt '''

import sqlify

sqlify.text_to_csv('tab_delim.txt', 'tab_delim.csv')
