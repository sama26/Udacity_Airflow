from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_conn = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        dq_checks = [
            {'sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result':0},
            {'sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result':0},
            {'sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result':0},
            {'sql': "SELECT COUNT(*) FROM songplays WHERE userid is null", 'expected_result':0},
            {'sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result':0}
            ]

        for check in dq_checks:
            sql = check.get('sql')
            expected_result = check.get('expected_result')

            count = 0
            records = redshift.get_records(sql)[0]

            if expected_result != records[0]:
                count += 1

            if count != 0:
                raise ValueError(f"Data quality check failed. {self.table} has nulls in primary key")

            if count == 0:
                logging.info(f"Data quality on table {self.table} check passed")
