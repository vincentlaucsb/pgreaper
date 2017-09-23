# '''
# Integration tests for SQLite, i.e. makes sure the database outputted
# is as you expect
# '''

# from pgreaper.testing import *
# import pgreaper

# import random
# import statistics
# import sqlite3

# class HelpersTest(unittest.TestCase):
    # ''' Tests of helper classes and functions '''
    
    # def test_assert_table(self):
        # x = 'harambe'
        # with self.assertRaises(TypeError):
            # pgreaper.table_to_sqlite(x, dbname='harambe.db')

# class BasicIntegrityTest(unittest.TestCase):
    # '''
    # Write a text file of random integers to an SQL database and 
    # verify the data has not been deformed
    # '''
    
    # @classmethod
    # def setUpClass(cls):
        # '''
        # Write the text file that will be converted to SQL
         # * Tab-delimited
         # * 500 rows + a row of headers
         # * 5 columns
         
        # Columns are:
         # 1. Random samples from a Uniform(0, 1) distribution
         # 2. Random samples from a Uniform(0, 10) distribution
         # 3. Random samples from a standard Normal distribution
         # 4. Random samples from a Normal(5, 25) distribution
         # 5. Random samples from an Exponential(1) distribution
        # '''
        
        # random.seed(1)
        
        # header = 'UNIFORM0_1\tUNIFORM0_10\tSTDNORM\tNORM5_25\tEXP_1\n'
        
        # Create columns
        # uniform0_1 = [random.uniform(0, 1) for i in range(0, 500)]
        # uniform0_10 = [random.uniform(0, 10) for i in range(0, 500)]
        # stdnorm = [random.normalvariate(0, 1) for i in range(0, 500)]
        # norm5_25 = [random.normalvariate(5, 25) for i in range(0, 500)]
        # exp_1 = [random.expovariate(1) for i in range(0, 500)]
        
        # cls.uni0_1_mean = statistics.mean(uniform0_1)
        # cls.uni0_10_mean = statistics.mean(uniform0_10)
        # cls.stdnorm_mean = statistics.mean(stdnorm)
        # cls.norm5_25_mean = statistics.mean(norm5_25)
        # cls.exp_1_mean = statistics.mean(exp_1)
        
        # with open('sqlite_test.txt', 'w') as test_file:
            # test_file.write(header)
            
            # for i in range(0, 500):
                # test_file.write('{0}\t{1}\t{2}\t{3}\t{4}\n'.format(
                    # uniform0_1[i], uniform0_10[i], stdnorm[i], norm5_25[i],
                    # exp_1[i]))
                    
        # Create the database
        # pgreaper.text_to_sqlite(
            # 'sqlite_test.txt',
            # dbname='sqlite_test.db',
            # name="random_numbers",
            # delimiter='\t')
    
    # def test_data_integrity(self):
        # '''
        # Test that the values of the columns are the same
        # (Fun fact: These tests might fail if you round further than the 10th sig fig)
        # '''
        
        # with sqlite3.connect('sqlite_test.db') as conn:
            
            # '''
            # Example output of sql_uni0_1_mean:
            # >>> [(0.4994879408040878,)]
            # '''

            # sql_uni0_1_mean = conn.execute(
                # "SELECT AVG(UNIFORM0_1) FROM random_numbers").fetchall()[0][0]
            
            # self.assertEqual(round(sql_uni0_1_mean, 5),
                             # round(BasicIntegrityTest.uni0_1_mean, 5))
            
            # sql_uni0_10_mean = conn.execute(
                # "SELECT AVG(UNIFORM0_10) FROM random_numbers").fetchall()[0][0]
            
            # self.assertEqual(round(sql_uni0_10_mean, 5),
                             # round(BasicIntegrityTest.uni0_10_mean, 5))
            
            # sql_stdnorm_mean = conn.execute(
                # "SELECT AVG(STDNORM) FROM random_numbers").fetchall()[0][0]
            
            # self.assertEqual(round(sql_stdnorm_mean, 5),
                             # round(BasicIntegrityTest.stdnorm_mean, 5))
            
            # sql_norm5_25_mean = conn.execute(
                # "SELECT AVG(NORM5_25) FROM random_numbers").fetchall()[0][0]
            
            # self.assertEqual(round(sql_norm5_25_mean, 5),
                             # round(BasicIntegrityTest.norm5_25_mean, 5))
            
            # sql_exp_1_mean = conn.execute(
                # "SELECT AVG(EXP_1) FROM random_numbers").fetchall()[0][0]
            
            # self.assertEqual(round(sql_exp_1_mean, 5),
                             # round(BasicIntegrityTest.exp_1_mean, 5))
        
    # Remove files created
    # @classmethod
    # def tearDownClass(cls):
        # os.remove('sqlite_test.txt')
        # os.remove('sqlite_test.db')
        
# class OverflowTest(unittest.TestCase):
    # ''' Test that integer overflows are handled appropriately '''
    
    # def setUp(self):
        # '''
           # Capital       Country       Currency      Demonym      Population
             # text      text prima..      text          text          text
        # -----------------------------------------------------------------------
         # 'Washington'     'USA'         'USD'       'American'    324774000
           # 'Moscow'      'Russia'       'RUB'       'Russian'     144554993
           # 'Ottawa'      'Canada'       'CAD'       'Canadian'   1111111111..
           
        # '''        
        # self.data = world_countries_table()
        # self.data.guess_type()
        # self.data.p_key = 'Country'
        # self.data[2][4] = 11111111111111111111111111
        
        # Will throw an error if large integers are not handled properly
        # pgreaper.table_to_sqlite(self.data, name='overflow', dbname='overflow.sqlite')
        
    # def test_integrity(self):
        # correct = [
            # (324774000,),
            # (144554993,),
            # (1.111111111111111e+25,),
        # ]
    
        # with sqlite3.connect('overflow.sqlite') as conn:
            # results = conn.execute('SELECT population FROM overflow')
            # self.assertEqual(results.fetchall(), correct)
        
    # @classmethod
    # def tearDownClass(cls):
        # os.remove('overflow.sqlite')
        
# class ZIPTest(unittest.TestCase):
    # ''' Test that loading from a compressed file works '''
    
    # @classmethod
    # def setUpClass(cls):
        # zip_file = pgreaper.read_zip(os.path.join('data', 'us_states.zip'))
        # pgreaper.csv_to_sqlite(zip_file['us_states.csv'],
            # name='us_states', dbname='sqlite_zip_test.db')
        
    # def test_integrity(self):
        # ''' Basic Data Integrity Checks '''
    
        # with sqlite3.connect('sqlite_zip_test.db') as conn:
            # num_states = conn.execute(
                # 'SELECT count(*) FROM us_states').fetchall()[0][0]
            
            # 50 States + DC
            # self.assertEqual(num_states, 51)
        
    # @classmethod
    # def tearDownClass(cls):
        # os.remove('sqlite_zip_test.db')
        
# if __name__ == '__main__':
    # unittest.main()