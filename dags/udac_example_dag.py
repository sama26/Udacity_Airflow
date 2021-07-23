from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

from operators.data_quality import DataQualityOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.stage_redshift import StageToRedshiftOperator

from loader_subdag import LoaderSubdag
                                
from helpers.sql_queries import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 7, 20),
    'depends_on_past':False,
    'email_on_failure':True,
    'email_on_retry':False,
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'catchup':False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    table_name="staging_events",
    s3_bucket = s3_bucket,
    s3_key = log_s3_key,
    file_format = "JSON",
    log_file = log_file,
    provide_context=True
    )
    
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    table_name="staging_songs",
    s3_bucket = s3_bucket,
    s3_key = song_s3_key,
    file_format = "JSON",
    provide_context=True
    )
    
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    subdag=load_fact_subdag(
        parent_dag_name=dag_name,
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        table="songplays",
        create_sql_stmt=SqlQueries.songplay_table_insert,
        append_or_replace = "append"
    	)
	)

load_user_dimension_table = SubDagOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    subdag=load_dimension_subdag(
        parent_dag_name=dag_name,
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        table="users",
        create_sql_stmt=SqlQueries.user_table_insert,
        append_or_replace = "replace"
    	)
	)

load_song_dimension_table = SubDagOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    subdag=load_dimension_subdag(
        parent_dag_name=dag_name,
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        table="songs",
        create_sql_stmt=SqlQueries.song_table_insert,
        append_or_replace = "replace"
    	)
	)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    subdag=load_dimension_subdag(
        parent_dag_name=dag_name,
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        table="artists",
        create_sql_stmt=SqlQueries.artist_table_insert,
        append_or_replace = "replace"
    	)
	)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    subdag=load_dimension_subdag(
        parent_dag_name=dag_name,
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        table="time",
        create_sql_stmt=SqlQueries.artist_table_insert,
        append_or_replace = "replace"
    	)
	)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift 

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
