''' Get a file of Postgres reserved key words which cannot be used as column names'''

import sqlify

url = 'https://www.postgresql.org/docs/current/static/sql-keywords-appendix.html'

keywords = sqlify.html.from_url(url)
keywords_table = keywords[1].groupby('PostgreSQL')

reserved_table = keywords_table['reserved'] + \
    keywords_table['reserved (can be function or type)']
reserved_keywords = reserved_table['Key Word']

with open('pg_keywords.txt', mode='w') as file:
    for kw in reserved_keywords:
        file.write('{}\n'.format(kw))