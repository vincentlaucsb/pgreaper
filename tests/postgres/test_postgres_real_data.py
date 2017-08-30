''' Integration tests for PostgreSQL with real data sets '''

import pgreaper
import pgreaper
from pgreaper.testing import *
from pgreaper import read_pg

class CAEmployeesTest(PostgresTestCase):
    ''' Test uploading a compressed file '''
    
    drop_tables = ['ca_employees', 'ca_employees_reject']
    
    @classmethod
    def setUpClass(self):
        zip_file = pgreaper.read_zip(os.path.join(
            REAL_DATA_DIR, '2015_StateDepartment.zip'))
        
        pgreaper.copy_csv(zip_file[0],
            name='ca_employees',
            dbname='pgreaper_pg_test')
            
    def test_row_count(self):
        ''' Do some basic integrity checks '''
        count = pgreaper.read_pg('SELECT count(*) from ca_employees as COUNT',
            dbname='pgreaper_pg_test')
        count_reject = pgreaper.read_pg('SELECT count(*) from ca_employees_reject as COUNT',
            dbname='pgreaper_pg_test')
            
        self.assertEqual(count['count'][0], 246486)
        self.assertEqual(count_reject['count'][0], 11)
        
    def test_schema(self):
        ''' Make sure the correct schema was loaded '''
        schema = pgreaper.postgres.get_table_schema('ca_employees', dbname='pgreaper_pg_test')
        
        self.assertEqual(schema.col_names, ['year', 'entity_type', 'entity_group',
            'entity_name', 'department_subdivision', 'position',
            'elected_official', 'judicial', 'other_positions',
            'min_classification_salary', 'max_classification_salary',
            'reported_base_wage', 'regular_pay', 'overtime_pay', 'lump_sum_pay',
            'other_pay', 'total_wages', 'defined_benefit_plan_contribution',
            'employees_retirement_cost_covered', 'deferred_compensation_plan',
            'health_dental_vision', 'total_retirement_and_health_cost',
            'pension_formula', 'entity_url', 'entity_population',
            'last_updated', 'entity_county', 'special_district_activities'])
        self.assertEqual(schema.col_types, ['bigint', 'text', 'text', 'text',
            'text', 'text', 'text', 'text', 'text', 'double precision',
            'double precision', 'text', 'double precision', 'double precision',
            'double precision', 'double precision', 'double precision',
            'double precision', 'bigint', 'bigint', 'double precision',
            'double precision', 'text', 'text', 'text', 'text', 'text', 'text'])

class PlacesTest(PostgresTestCase):
    ''' Test uploading a compressed TXT of US places '''
    
    drop_tables = ['places']
    
    @classmethod
    def setUpClass(self):
        zip_file = pgreaper.read_zip(os.path.join(
            REAL_DATA_DIR, '2016_Gaz_place_national.zip'))
        pgreaper.copy_csv(zip_file[0], name='places',
            delimiter='\t', dbname='pgreaper_pg_test')
            
    def test_row_count(self):
        ''' Make sure all rows were loaded '''
        count = pgreaper.read_pg('SELECT count(*) from places as COUNT',
            dbname='pgreaper_pg_test')
        self.assertEqual(count['count'][0], 29575)
        
    def test_schema(self):
        ''' Make sure the correct schema was loaded '''
        schema = pgreaper.postgres.get_table_schema('places', dbname='pgreaper_pg_test')        
        self.assertEqual(schema.col_names,
            ['usps', 'geoid', 'ansicode', 'name', 'lsad', 'funcstat',
            'aland', 'awater', 'aland_sqmi', 'awater_sqmi', 'intptlat',
            'intptlong'])
        self.assertEqual(schema.col_types,
            ['text', 'bigint', 'bigint', 'text', 'text', 'text', 'bigint',
            'bigint', 'double precision', 'double precision',
            'double precision', 'text'])
        
if __name__ == '__main__':
    unittest.main()