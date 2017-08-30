import pgreaper
import timeit

def load():
    my_zip = pgreaper.read_zip('2016_Gaz_place_national.zip')
    pgreaper.copy_csv(my_zip[0], name='places',
        dbname='sqlify_pg_test', delimiter='\t')
        
if __name__ == '__main__':
    print(timeit.timeit(load, globals=globals(), number=1))