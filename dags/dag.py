from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from resources.lake import create_buckets
from resources.extract import fetch_data_from_api, store_data_on_s3

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "chore_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)
create_lake = PythonOperator(
    task_id="task1",
    python_callable=create_buckets,
    dag=dag,
)
extract_from_api = PythonOperator(
    task_id="task2",
    python_callable=fetch_data_from_api,
    dag=dag,
)
save_on_s3 = PythonOperator(
    task_id="task3",
    python_callable=store_data_on_s3,
    dag=dag,
)
convert_data = SparkSubmitOperator(
    task_id="task4",
    application="resources/transform.py",
    dag=dag,
)
visualize_data = SparkSubmitOperator(
    task_id="task5",
    application="resources/visualize.py",
    dag=dag,
)

create_lake >> extract_from_api >> save_on_s3 >> convert_data >> visualize_data
