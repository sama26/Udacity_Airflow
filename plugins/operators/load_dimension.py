from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 create_sql_stmt = "",
                 replace = True,
                 table = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_sql_stmt = create_sql_stmt
        self.table = table
        self.replace = replace

    def execute(self, context):
        redshift_conn = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if replace:
            redshift_conn.run(f"DELETE * FROM {self.table}")
            self.log.info(f"Existing {self.table} table deleted ready for replace operation")

        redshift_conn.run(self.create_sql_stmt)
        self.log.info(f"{self.table} successfully loaded in {self.apprend_or_replace} operation")
