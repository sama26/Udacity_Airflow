from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from operators.load_dimension import LoadDimensionOperator

from helpers.sql_queries import SqlQueries


def loader_subdag(
    parent_dag_name,
    task_id,
    redshift_conn_id,
    table,
    create_sql_stmt,
    replace,
    *args, **kwargs):

    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
        )

    loader = LoadDimensionOperator(
        task_id=task_id,
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table = table,
        create_sql_stmt = create_sql_stmt,
        replace = replace
        )

    loader

    return dag
