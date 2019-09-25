from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    truncate_sql="""
    TRUNCATE TABLE {table}
    """    
    
    @apply_defaults
    def __init__(self,
                 conn_id="redshift",
                 table="",
                 sql="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        """
        Insert data into dimensional tables from staging events and staging song tables.
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


        # Truncating table
        if not self.append_only:
            self.log.info(f"Truncating table {self.table}")
            postgres_hook.run(LoadDimensionOperator.truncate_sql.format(table=self.table))
        
        # Inserting data from staging table into dimension
        custom_sql = f"INSERT INTO {self.table} ({self.sql})"
        postgres_hook.run(custom_sql)        
        self.log.info(f"Success: Inserting values on {self.table}, {self.task_id} loaded.")
