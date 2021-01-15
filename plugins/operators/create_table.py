from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        redshift_conn_id,
        query,
        *args,
        **kwargs,
    ):

        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        self.log.info("Connecting to data")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info("Creating table")
        redshift_hook.run(self.query)
