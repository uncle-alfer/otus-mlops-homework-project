
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

# copy_cmd = "hadoop distcp -D fs.s3a.bucket.dataproc-examples.endpoint=storage.yandexcloud.net -D fs.s3a.bucket.dataproc-examples.access.key=$ACCESS -D fs.s3a.bucket.dataproc-examples.secret.key=$SECRET -update -skipcrccheck -numListstatusThreads 10  s3a://mlops-hw3-vos/raw_data/ hdfs://rc1a-dataproc-m-g7gq6h57ys820c43.mdb.yandexcloud.net/user/root/datasets/set02/"

# copy_from_s3_to_hdfs = SSHOperator(
#     task_id="copy_from_s3_to_hdfs",
#     command=copy_cmd,
#     ssh_hook=sshHook,
#     dag=dag_spark,
# )

spark_submit = SSHOperator(
    task_id="spark_submit",
    command="~/otus-mlops-homework-project/notebooks/hw4/3_flights_pipe_withHP_solution.sh",
    ssh_hook=sshHook,
    dag=dag_spark,
)


send_file >> copy_from_s3_to_hdfs  >> spark_submit

if __name__ == "__main__":
    dag_spark.cli()
