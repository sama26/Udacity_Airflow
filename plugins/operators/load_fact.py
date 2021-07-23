from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                create_sql_stmt = "",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_sql_stmt = create_sql_stmt


    def execute(self, context):
        redshift_conn = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        redshift_conn.run(self.create_sql_stmt)
        self.log.info('Fact table transformation and loading complete')
