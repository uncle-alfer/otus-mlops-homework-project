import airflow
from datetime import timedelta
from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHHook, SSHOperator
# from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# sshHook = SSHHook(conn_id="ssh_default", key_file="/home/airflow/keys/ssh.key")
sshHook = SSHHook(remote_host='', username='worker', key_file='/home/airflow/keys/ssh.key')

default_args = {
    'owner': 'airflow',
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_spark = DAG(
    dag_id = "sparkoperator1",
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule_interval='@once',
    dagrun_timeout=timedelta(minutes=10),
    description='use case of sparkoperator in airflow',
    start_date = airflow.utils.dates.days_ago(1)
)

print("send_file_to_otjer_vm")
send_file = SFTPOperator(
    task_id="send_file",
    ssh_conn_id="ssh_default",
    local_filepath="/home/airflow/utils/cleanse_data.py",
    remote_filepath="/home/worker/utils/cleanse_data.py",
    operation="put",
    dag=dag_spark
)

run_cmd = SSHOperator(
    task_id="run_cmd",
    command="touch FOOOOCK.txt",
    ssh_hook=sshHook,
    dag=dag_spark,
)

send_file >> run_cmd

if __name__ == "__main__":
    dag_spark.cli()
