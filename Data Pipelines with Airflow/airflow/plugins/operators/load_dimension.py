from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Airflow Operator to load dimension tables
    into AWS Redshift Cluster
        
    Arguments required:
        - redshift_conn_id: Airflow AWS 
            credentials name
        - table: name of table to load data into
        - sql: insert query for specified table
        - insert_mode: method for insertion 
            (truncate_insert or append)
    """

    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 insert_mode="append",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.insert_mode = insert_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        formatted_sql = "INSERT INTO {} ".format(self.table)
        
        if self.insert_mode == "truncate_insert":
            self.log.info("Deleting records from: ", self.table)
            redshift.run("DELETE FROM {}".format(self.table))
        elif self.insert_mode != "append":
            self.log.info("Invalid insert_mode, running with default append method")

        self.log.info("Inserting records into: ", self.table)
        redshift.run(formatted_sql + self.sql)
