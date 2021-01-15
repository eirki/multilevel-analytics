from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class s3ToRedshiftOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        aws_credentials,
        redshift_conn_id,
        table,
        filename,
        query,
        *args,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)
        self.aws_credentials = aws_credentials
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.filename = filename
        self.query = query

    def execute(self, context):
        """
        Moves data from a .csv file S3 to a redshift table
        """
        self.log.info("Connecting to database")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info("Authenticating ")
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        bucket_name = "ebs-capstone-bucket"
        s3_path = f"s3://{bucket_name}/{self.filename}"
        formatted_sql = self.query.format(
            table=self.table,
            path=s3_path,
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key,
        )
        self.log.info("Copying data from s3")
        redshift_hook.run(formatted_sql)
