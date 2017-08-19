import sqlify
import os

data = sqlify.read_json(os.path.join('data', 'twitter', 'congress_tweets.json'))

sqlify.json_to_pg(os.path.join('data', 'twitter', 'congress_tweets.json'), name='congress_tweets', dbname='sqlify_pg_test')