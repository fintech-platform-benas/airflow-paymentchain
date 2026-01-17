from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'benas',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'payment_processor',
    default_args=default_args,
    description='Process payment transactions with PySpark from S3',
    schedule_interval=None,
    catchup=False,
    tags=['pyspark', 'payments', 'batch', 's3'],
)

# Task: Ejecutar script PySpark S3
run_pyspark_job = BashOperator(
    task_id='run_pyspark_processor',
    bash_command='cd /opt/airflow/payment_processor && python process_transactions.py',
    dag=dag,
)

# Task: Notificar éxito
notify_success = BashOperator(
    task_id='notify_success',
    bash_command='echo "✅ Batch processing completed successfully"',
    dag=dag,
)

# Dependencies
run_pyspark_job >> notify_success