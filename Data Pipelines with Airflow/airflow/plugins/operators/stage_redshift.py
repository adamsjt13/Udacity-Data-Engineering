from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Airflow Operator to copy data from
    AWS s3 bucket into AWS Redshift cluster
    
    Required Arguments:
        redshift_conn_id: Airflow AWS 
            credentials name
        aws_credentials: Airflow credentials
            set up for 
        table: name of table to copy data into
        s3_bucket: s3 bucket location to find data
        s3_key: path in s3 bucket to find data
        json_format: file path to file that
            specifies format for json load or 'auto'
    """
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        """.format(self.table,
                   s3_path,
                   credentials.access_key,
                   credentials.secret_key,
                   self.json_format)
        
        self.log.info("S3 File(s): ", rendered_key, " being copied into ", self.table)
        redshift.run(formatted_sql)





