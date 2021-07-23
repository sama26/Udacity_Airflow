from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
                                
from helpers.sql_queries import SqlQueries


def loader_subdag(
    	parent_dag_name,
    	task_id,
    	redshift_conn_id,
    	table,
    	create_sql_stmt,
    	append_or_replace,
    	*args, **kwargs):
    dag = DAG(
    	f"{parent_dag_name}.{task_id}",
    	**kwargs
    	)
    
    if task_id = "Load_songplays_fact_table":
    	loader = LoadFactOperator(
       		task_id=task_id,
       		dag=dag,
       		redshift_conn_id=redshift_conn_id,
       		table = table,
       		create_sql_stmt = create_sql_stmt,
      	 	append_or_replace = append_or_replace
			)    
    else:
    	loader = LoadDimensionOperator(
        	task_id=task_id,
        	dag=dag,
        	redshift_conn_id=redshift_conn_id,
        	table = table,
        	create_sql_stmt = create_sql_stmt,
        	append_or_replace = append_or_replace
    		)    
    
    loader
    
    return dag

