from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id="redshift",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        """
        Insert data into fact tables from staging events and staging song tables.
        Parameters:
        ----------
        conn_id: string
            airflow connection to redshift cluster
        table: string
            target table located in redshift cluster
        sql: string
            SQL command to generate insert data.
        """
        postgres_hook = PostgresHook(self.conn_id)
        custom_sql = f"INSERT INTO {self.table} ({self.sql})"
        postgres_hook.run(custom_sql)
        self.log.info(f"Success: {self.task_id} loaded.")
