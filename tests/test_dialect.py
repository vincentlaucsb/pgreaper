'''
Tests of helper classes (and their subclasses) from
 * sqlify.core.schema
 * sqlify.core.dbapi
'''

from sqlify.postgres.conn import postgres_connect
import sqlify

from os import path
import unittest
import psycopg2

class SQLiteTest(unittest.TestCase):
    '''
    Test if DialectSQLite class works properly
     * Test if data type guesser is reasonably accurate
     * Test other features
    '''
    
    dialect = sqlify.core.schema.DialectSQLite()
    
    def test_eq(self):
        self.assertEqual(SQLiteTest.dialect, 'sqlite')
    
    def test_obvious_case1(self):
        input = '3.14'
        output = SQLiteTest.dialect.guess_data_type(input)
        expected_output = 'REAL'
        
        self.assertEqual(output, expected_output)
        
    def test_obvious_case2(self):
        input = 'Tom Brady'
        output = SQLiteTest.dialect.guess_data_type(input)
        expected_output = 'TEXT'
        
        self.assertEqual(output, expected_output)
        
    def test_obvious_case3(self):
        input = '93117'
        output = SQLiteTest.dialect.guess_data_type(input)
        expected_output = 'INTEGER'
        
        self.assertEqual(output, expected_output)
        
class PostgresTest(unittest.TestCase):
    '''
    Test if DialectPostgres class works properly
     * Test if data type guesser is reasonably accurate
     * Test other features
    '''
    
    dialect = sqlify.core.schema.DialectPostgres()
    
    def test_eq(self):
        self.assertEqual(PostgresTest.dialect, 'postgres')
    
    def test_obvious_case1(self):
        input = '3.14'
        output = PostgresTest.dialect.guess_data_type(input)
        expected_output = 'DOUBLE PRECISION'
        
        self.assertEqual(output, expected_output)
        
    def test_obvious_case2(self):
        input = 'Tom Brady'
        output = PostgresTest.dialect.guess_data_type(input)
        expected_output = 'TEXT'
        
        self.assertEqual(output, expected_output)
        
    def test_obvious_case3(self):
        input = '93117'
        output = PostgresTest.dialect.guess_data_type(input)
        expected_output = 'BIGINT'
        
        self.assertEqual(output, expected_output)
        
if __name__ == '__main__':
    unittest.main()