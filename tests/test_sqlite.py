'''
Integration tests for SQLite, i.e. makes sure the database outputted
is as you expect
'''

import sqlify

import random
import statistics
import os
import unittest
import sqlite3

class BasicIntegrityTest(unittest.TestCase):
    '''
    Write a text file of random integers to an SQL database and 
    verify the data has not been deformed
    '''
    
    @classmethod
    def setUpClass(cls):
        '''
        Write the text file that will be converted to SQL
         * Tab-delimited
         * 500 rows + a row of headers
         * 5 columns
         
        Columns are:
         1. Random samples from a Uniform(0, 1) distribution
         2. Random samples from a Uniform(0, 10) distribution
         3. Random samples from a standard Normal distribution
         4. Random samples from a Normal(5, 25) distribution
         5. Random samples from an Exponential(1) distribution
        '''
        
        random.seed(1)
        
        header = 'UNIFORM0_1\tUNIFORM0_10\tSTDNORM\tNORM5_25\tEXP_1\n'
        
        # Create columns
        uniform0_1 = [random.uniform(0, 1) for i in range(0, 500)]
        uniform0_10 = [random.uniform(0, 10) for i in range(0, 500)]
        stdnorm = [random.normalvariate(0, 1) for i in range(0, 500)]
        norm5_25 = [random.normalvariate(5, 25) for i in range(0, 500)]
        exp_1 = [random.expovariate(1) for i in range(0, 500)]
        
        cls.uni0_1_mean = statistics.mean(uniform0_1)
        cls.uni0_10_mean = statistics.mean(uniform0_10)
        cls.stdnorm_mean = statistics.mean(stdnorm)
        cls.norm5_25_mean = statistics.mean(norm5_25)
        cls.exp_1_mean = statistics.mean(exp_1)
        
        with open('sqlite_test.txt', 'w') as test_file:
            test_file.write(header)
            
            for i in range(0, 500):
                test_file.write('{0}\t{1}\t{2}\t{3}\t{4}\n'.format(
                    uniform0_1[i], uniform0_10[i], stdnorm[i], norm5_25[i],
                    exp_1[i]))
                    
        # Create the database
        sqlify.text_to_sqlite(
            'sqlite_test.txt',
            database='sqlite_test.db',
            name="random_numbers",
            delimiter='\t')
    
    def test_data_integrity(self):
        '''
        Test that the values of the columns are the same
        (Fun fact: These tests might fail if you round further than the 10th sig fig)
        '''
        
        conn = sqlite3.connect('sqlite_test.db')
        
        '''
        Example output of sql_uni0_1_mean:
        >>> [(0.4994879408040878,)]
        '''

        sql_uni0_1_mean = conn.execute(
            "SELECT AVG(UNIFORM0_1) FROM random_numbers").fetchall()[0][0]
        
        self.assertEqual(round(sql_uni0_1_mean, 5),
                         round(BasicIntegrityTest.uni0_1_mean, 5))
        
        sql_uni0_10_mean = conn.execute(
            "SELECT AVG(UNIFORM0_10) FROM random_numbers").fetchall()[0][0]
        
        self.assertEqual(round(sql_uni0_10_mean, 5),
                         round(BasicIntegrityTest.uni0_10_mean, 5))
        
        sql_stdnorm_mean = conn.execute(
            "SELECT AVG(STDNORM) FROM random_numbers").fetchall()[0][0]
        
        self.assertEqual(round(sql_stdnorm_mean, 5),
                         round(BasicIntegrityTest.stdnorm_mean, 5))
        
        sql_norm5_25_mean = conn.execute(
            "SELECT AVG(NORM5_25) FROM random_numbers").fetchall()[0][0]
        
        self.assertEqual(round(sql_norm5_25_mean, 5),
                         round(BasicIntegrityTest.norm5_25_mean, 5))
        
        sql_exp_1_mean = conn.execute(
            "SELECT AVG(EXP_1) FROM random_numbers").fetchall()[0][0]
        
        self.assertEqual(round(sql_exp_1_mean, 5),
                         round(BasicIntegrityTest.exp_1_mean, 5))
        
        conn.close()
        
    # Remove files created
    @classmethod
    def tearDownClass(cls):
        os.remove('sqlite_test.txt')
        os.remove('sqlite_test.db')
        
if __name__ == '__main__':
    unittest.main()