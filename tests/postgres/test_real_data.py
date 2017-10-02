''' Integration tests for PostgreSQL with real data sets '''

import pgreaper
import pgreaper
from pgreaper.testing import *
from pgreaper import read_pg

# CSV is malformed (extra header columns) --> Will have to deal with later
class CAEmployeesTest(PostgresTestCase):
    ''' Test uploading a compressed file '''
    
    drop_tables = ['ca_employees']
    
    @classmethod
    def setUpClass(self):
        pgreaper.copy_csv(os.path.join(
            REAL_CSV_DATA, '2015_StateDepartment.csv'),
            name='ca_employees',
            dbname=TEST_DB)
            
    def test_row_count(self):
        ''' Do some basic integrity checks '''
        count = pgreaper.read_pg(
            'SELECT count(*) from ca_employees as COUNT',
            dbname=TEST_DB)
        self.assertEqual(count['count'][0], 246497)
        
    def test_schema(self):
        ''' Make sure the correct schema was loaded '''
        schema = pgreaper.postgres.get_table_schema('ca_employees', dbname=TEST_DB)
        
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
            
        self.assertEqual(schema.col_types, ['bigint', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'text', 'double precision', 'double precision', 'text', 'double precision', 'double precision', 'double precision', 'double precision', 'double precision', 'double precision', 'double precision', 'bigint', 'double precision', 'double precision', 'text', 'text', 'text', 'text', 'text', 'text'])

# Stupid encoding error
# class PlacesTest(PostgresTestCase):
    # ''' Test uploading a compressed TXT of US places '''
    
    # drop_tables = ['places']
    
    # @classmethod
    # def setUpClass(self):
        # pgreaper.copy_csv(os.path.join(
            # REAL_CSV_DATA, '2016_Gaz_place_national.txt'), name='places',
            # delimiter='\t', dbname=TEST_DB, encoding='cp1252')
            
    # def test_count(self):
        # ''' Make sure all rows were loaded '''
        # self.assertCount('places', 29575)
        
    # def test_schema(self):
        # ''' Make sure the correct schema was loaded '''
        # schema = pgreaper.postgres.get_table_schema('places', dbname=TEST_DB)        
        # self.assertEqual(schema.col_names,
            # ['usps', 'geoid', 'ansicode', 'name', 'lsad', 'funcstat',
            # 'aland', 'awater', 'aland_sqmi', 'awater_sqmi', 'intptlat',
            # 'intptlong'])
        # self.assertEqual(schema.col_types,
            # ['text', 'bigint', 'bigint', 'text', 'text', 'text', 'bigint',
            # 'bigint', 'double precision', 'double precision',
            # 'double precision', 'text'])
        
if __name__ == '__main__':
    unittest.main()