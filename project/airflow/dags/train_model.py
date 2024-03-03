import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id = "1_train_model",
    default_args=default_args,
    schedule_interval='0 4 * * *',
    dagrun_timeout=timedelta(minutes=100),
    description='train model',
    start_date = airflow.utils.dates.days_ago(1)
)

install_deps = BashOperator(
    task_id='install_deps',
    bash_command='pip install xgboost mlflow',
    dag=dag
)

train_model = BashOperator(
    task_id='train_model',
    bash_command=(
        'AWS_SHARED_CREDENTIALS_FILE=/opt/airflow/keys/credentials '
        'AWS_CONFIG_FILE=/opt/airflow/keys/config python3 /opt/airflow/utils/train_model.py'
    ),
    dag=dag
)

install_deps >> train_model

if __name__ == "__main__":
    dag.cli()