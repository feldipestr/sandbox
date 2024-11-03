import os
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator


dag_id = os.path.basename(__file__).split('.')[0]
dag_description = 'Prepare source data.'
schedule_interval = '30 */1 * * *'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['user@domain.com'],
    'email_on_fail': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id=dag_id,
         default_args=default_args,
         schedule_interval=schedule_interval,
         catchup=False,
         description=dag_description,
         max_active_runs=1) as dag:

    job1_task = BashOperator(
        task_id=f'{dag_id}-job-1-prepare-data',
        bash_command='python ~/<project>/pipelines/job1/job1_prepare_data.py',
        dag=dag,
    )

    job1_task
