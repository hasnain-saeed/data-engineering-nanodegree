from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        insert into {}
        {};
    """
    truncate_sql = """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 sql="",
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.truncate_table = truncate_table
        self.sql = sql

    def execute(self, context):
        self.log.info("Getting redshift connection")
        redshift = PostgresHook(self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f"Truncating dimension table: {self.target_table}")
            redshift.run(LoadDimensionOperator.truncate_sql.format(self.target_table))
        
        load_sql = LoadDimensionOperator.insert_sql.format(
            self.target_table,
            self.sql
        )
        self.log.info(f"Inserting Data - to {self.target_table} ")
        redshift.run(load_sql)
