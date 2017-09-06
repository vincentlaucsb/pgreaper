from pgreaper.postgres.json_loader import copy_json

tbl = copy_json('persons.json', name='persons2', dbname='pgreaper_test')