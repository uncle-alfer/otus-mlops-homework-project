import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago

sshHook = SSHHook(remote_host='', username='ubuntu', key_file='/opt/airflow/keys/ssh.key', cmd_timeout=None, conn_timeout = None)

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id = "validate_model",
    default_args=default_args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=10),
    description='use case of model valdation',
    start_date = airflow.utils.dates.days_ago(1)
)

validate = SSHOperator(
    task_id="validate",
    command="python /home/ubuntu/utils/cleanse_data.py",
    ssh_hook=sshHook,
    dag=dag,
)

validate

if __name__ == "__main__":
    dag.cli()
