from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        self.log.info("Getting redshift connection")
        redshift = PostgresHook(self.redshift_conn_id)
        errors = 0
        
        for quality_check in self.data_quality_checks:
            data_check_query = quality_check.get('data_check_query')
            expected_output = quality_check.get('expected_output')
            output = redshift.get_records(data_check_query)[0]
            
            self.log.info(f"Running         : {data_check_query}")
            
            if output[0] != expected_output:
                errors += 1
                self.log.info(f"Data quality check failed At   : {data_check_query}")
            
            self.log.info(f"Output          : {output}")
            self.log.info(f"Expected output : {expected_output}")
            
        if errors > 0:
            raise ValueError('Data quality check failed')
            
        self.log.info('Data quality checks passed')