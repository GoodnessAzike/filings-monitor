from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id = "ping_warehouse",
    start_date=datetime(2026, 10, 25),
    schedule=None,
):
    #the task 
    SQLExecuteQueryOperator(
        task_id="ping",
        conn_id="warehouse",
        sql="SELECT 1"
    )