from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Airflow Operator to run the following
    basic data quality checks for Songify data:
        - ensure that data was properly loaded 
            (> 0 rows in table)
        - ensure that specified columns do not
        contain NULL rows
        
    Arguments required:
        - redshift_conn_id: Airflow AWS 
            credentials name
        - check_tables: list of tables to check 
            data loaded properly
        - check_table_columns: dictionary with 
            keys being table names and values
            being lists of column names in that
            table to check for NULLs
    """
    ui_color = '#89DA59'
    
    check_rows_sql = """
    SELECT COUNT(*)
    FROM {}
    """
    check_nulls_sql = """
    SELECT COUNT(*)
    FROM {}
    WHERE {} IS NULL
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check_tables=[],
                 check_table_columns={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_tables = check_tables
        self.check_table_columns = check_table_columns

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        #####################
        # CHECK DATA LOAD
        #####################
        self.log.info("Checking the following tables to ensure data was loaded: {}".format(self.check_tables))
        for table in self.check_tables:
            check_table_rows = DataQualityOperator.check_rows_sql.format(table)
            data = redshift.get_records(check_table_rows)
            num_records = data[0][0]
            if num_records < 1:
                raise ValueError("{} has no records".format(table))
            else:
                self.log.info("Data loaded successfully: {} table has {} records.".format(table, num_records))
        #####################
        # CHECK NULL COLUMNS
        #####################       
        self.log.info("Checking the following tables and columns to ensure data is not NULL: {}".format(self.check_table_columns))
        for table, columns in self.check_table_columns.items():
            for column in columns:
                check_nulls = DataQualityOperator.check_nulls_sql.format(table,column)
                data = redshift.get_records(check_nulls)
                num_records = data[0][0]                
                if num_records > 0:
                    raise ValueError("{} column in {} table has {} NULL records".format(column, table, num_records))
                else:
                    self.log.info("Data loaded successfully: {} column in {} table has 0 NULL records.".format(column,table))