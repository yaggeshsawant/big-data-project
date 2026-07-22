from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'data_ops',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='kafka_stream_workflow',
    default_args=default_args,
    schedule_interval=None,   # strictly event-driven, only runs when triggered
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,        
) as dag:

    # 1. Producer (Python script)
    run_producer = BashOperator(
        task_id='run_kafka_producer',
        bash_command='python3 /home/talentum/Desktop/airflow-tutorial/spark-app/producer_hdfs_to_kafka.py',
        execution_timeout=timedelta(minutes=15),
    )

    # 2. Consumer (Python script)
    run_consumer = BashOperator(
        task_id='run_kafka_consumer',
        bash_command='python3 /home/talentum/Desktop/airflow-tutorial/spark-app/consumer_kafka_to_hdfs.py',
        execution_timeout=timedelta(minutes=20),
    )

    # 3. Preprocessing (PySpark job)
    run_preprocessing = BashOperator(
        task_id='preprocessing',
        bash_command='spark-submit /home/talentum/Desktop/airflow-tutorial/spark-app/preprocess_clean.py',
        execution_timeout=timedelta(minutes=15),
    )

    # 4. Transformation (PySpark job)
    run_transformation = BashOperator(
        task_id='transformation',
        bash_command='spark-submit /home/talentum/Desktop/airflow-tutorial/spark-app/transfromation.py',
        execution_timeout=timedelta(minutes=20),
    )

    # 5. Export (PySpark job)
    run_export = BashOperator(
        task_id='export',
        bash_command='spark-submit /home/talentum/Desktop/airflow-tutorial/spark-app/export_data.py',
        execution_timeout=timedelta(minutes=10),
    )

    
    run_producer >> run_consumer >> run_preprocessing >> run_transformation >> run_export
