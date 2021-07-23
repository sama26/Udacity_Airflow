from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 create_sql_stmt = "",
                 append_or_replace = "",
                 table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_sql_stmt = create_sql_stmt
        self.table = table
        self.append_or_replace = append_or_replace


    def execute(self, context):
        redshift_conn = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        if append_or_replace = "replace"
    		redshift_conn.run(f"DELETE * FROM {self.table}")
  			self.log.info(f"Existing {self.table} table deleted ready for replace operation")
        
        redshift_conn.run(self.create_sql_stmt)
        self.log.info('Fact table transformation and loading complete')
