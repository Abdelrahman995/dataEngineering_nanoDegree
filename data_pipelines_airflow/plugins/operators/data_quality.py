from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for check_element in self.dq_checks :
            records = redshift_hook.get_records(check_element['check_sql'])
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {check_element['check_sql']} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {check_element['check_sql']} contained 0 rows")
            if num_records == check_element['expected_result']:
                self.log.info(f"Data quality statement {check_element['check_sql']} check passed with {records[0][0]} records")