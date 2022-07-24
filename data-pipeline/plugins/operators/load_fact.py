from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {}
        {}
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql = sql

    def execute(self, context):
        self.log.info("Getting redshift connection")
        redshift = PostgresHook(self.redshift_conn_id)
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.target_table,
            self.sql
        )
        redshift.run(formatted_sql)
        self.log.info(f"Inserting data to {self.target_table}")
