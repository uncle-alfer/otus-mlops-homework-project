
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago

sshHook = SSHHook(remote_host='84.252.130.8', username='ubuntu', key_file='/opt/airflow/keys/ssh.key', cmd_timeout=None, conn_timeout = None)

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
}

dag_spark = DAG(
    dag_id = "sparkoperator2",
    default_args=default_args,
    schedule_interval='0 1 * * *',
    dagrun_timeout=timedelta(minutes=10),
    description='use case of sparkoperator in airflow',
    start_date = airflow.utils.dates.days_ago(1)
)

spark_submit = SSHOperator(
    task_id="spark_submit",
    command="~/otus-mlops-homework-project/notebooks/hw4/3_flights_pipe_withHP_solution.sh",
    ssh_hook=sshHook,
    dag=dag_spark,
)


spark_submit

if __name__ == "__main__":
    dag_spark.cli()
