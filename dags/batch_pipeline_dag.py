from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="batch_pipeline",
    default_args=default_args,
    description="End-to-end batch pipeline: raw ingest -> transform -> validate",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 8),
    catchup=False,
    tags=["project-01"],
) as dag:

    raw_ingest = BashOperator(
        task_id="raw_ingest",
        # spark-submit runs the script the same way `python script.py` would,
        # but through Spark's own launcher (handles Spark configs properly)
        bash_command="python /opt/airflow/scripts/raw_ingest.py",
    )

    transform = BashOperator(
        task_id="transform",
         bash_command="python /opt/airflow/scripts/transform.py",
    )
    
    validate = BashOperator(
        task_id="validate",
         bash_command="python /opt/airflow/scripts/validate.py",
    )
    
    final_analytics = BashOperator(
        task_id="final_analytics",
         bash_command="python /opt/airflow/scripts/final_analytics.py",
    )

raw_ingest >> transform >> validate >> final_analytics
