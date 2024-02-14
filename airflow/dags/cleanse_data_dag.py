import airflow
from datetime import timedelta
from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.ssh.operators.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
# from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# sshHook = SSHHook(conn_id="ssh_default", key_file="/home/airflow/keys/ssh.key")
sshHook = SSHHook(remote_host='84.252.130.8', username='ubuntu', key_file='/opt/airflow/keys/ssh.key', cmd_timeout=None, conn_timeout = None)

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
    schedule_interval='0,30 * * * *',
    dagrun_timeout=timedelta(minutes=10),
    description='use case of sparkoperator in airflow',
    start_date = airflow.utils.dates.days_ago(1)
)

print("send_file_to_otjer_vm")
send_file = SFTPOperator(
    task_id="send_file",
    # ssh_conn_id="ssh_default",
    ssh_hook=sshHook,
    local_filepath="/opt/airflow/utils/cleanse_data.py",
    remote_filepath="/home/ubuntu/utils/cleanse_data.py",
    operation="put",
    create_intermediate_dirs=True,
    dag=dag_spark
)

copy_cmd = "hadoop distcp -D fs.s3a.bucket.dataproc-examples.endpoint=storage.yandexcloud.net -D fs.s3a.bucket.dataproc-examples.access.key=$ACCESS -D fs.s3a.bucket.dataproc-examples.secret.key=$SECRET -update -skipcrccheck -numListstatusThreads 10  s3a://mlops-hw3-vos/raw_data/ hdfs://rc1a-dataproc-m-g7gq6h57ys820c43.mdb.yandexcloud.net/user/root/datasets/set02/"

copy_from_s3_to_hdfs = SSHOperator(
    task_id="copy_from_s3_to_hdfs",
    command=copy_cmd,
    ssh_hook=sshHook,
    dag=dag_spark,
)

spark_submit = SSHOperator(
    task_id="spark_submit",
    command="spark-submit /home/ubuntu/utils/cleanse_data.py",
    # ssh_conn_id="ssh_default",
    ssh_hook=sshHook,
    dag=dag_spark,
)


send_file >> copy_from_s3_to_hdfs  >> spark_submit

if __name__ == "__main__":
    dag_spark.cli()
