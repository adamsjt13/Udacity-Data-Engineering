from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Airflow Operator to load dimension tables
    into AWS Redshift Cluster
        
    Arguments required:
        - redshift_conn_id: Airflow AWS 
            credentials name
        - table: name of table to load data into
        - sql: insert query for specified table
    """
    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        formatted_sql = "INSERT INTO {} ".format(self.table)
        redshift.run(formatted_sql + self.sql)
