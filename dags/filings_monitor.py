from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.standard.operators.python import PythonOperator
from include.sec_filings import extract_load
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator



with DAG(
    dag_id="filings_monitor",
    start_date=datetime(2026, 10, 25),
    schedule=None,
):

    extract_load_filings = PythonOperator(
        task_id="extract_load_filings",
        python_callable=extract_load,
    )

    transform_filings = SQLExecuteQueryOperator(
        task_id = "transform_filings",
        conn_id = "warehouse",
        sql = "sql/002_filings.sql",
        split_statements=True,
    )

    extract_load_filings >> transform_filings