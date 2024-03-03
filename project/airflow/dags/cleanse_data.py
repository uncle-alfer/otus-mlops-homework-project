import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id = "0_cleanse_data",
    default_args=default_args,
    schedule_interval='0 3 * * *',
    dagrun_timeout=timedelta(minutes=100),
    description='cleanse data',
    start_date = airflow.utils.dates.days_ago(1)
)

# install_deps = BashOperator(
#     task_id='install_deps',
#     bash_command='pip install scipy s3fs',
#     dag=dag
# )

cleanse_data = BashOperator(
    task_id='cleanse_data',
    bash_command=(
        'AWS_SHARED_CREDENTIALS_FILE=/opt/airflow/keys/credentials '
        'AWS_CONFIG_FILE=/opt/airflow/keys/config python3 /opt/airflow/utils/cleanse_data.py'
    ),
    dag=dag
)

# install_deps >> cleanse_data
cleanse_data

if __name__ == "__main__":
    dag.cli()