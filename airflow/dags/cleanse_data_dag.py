import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

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
        dag_id = "sparkoperator",
        default_args=default_args,
        # schedule_interval='0 0 * * *',
        schedule_interval='@once',	
        dagrun_timeout=timedelta(minutes=10),
        description='use case of sparkoperator in airflow',
        start_date = airflow.utils.dates.days_ago(1)
)


_bash_command = """hadoop distcp -D fs.s3a.bucket.dataproc-examples.endpoint=storage.yandexcloud.net -D fs.s3a.bucket.dataproc-examples.access.key=YCAJEB2u8asOP4dTzk6AZBrbJ -D fs.s3a.bucket.dataproc-examples.secret.key=YCPjXXHnwGim3NaPpRmkmJQmQq3V0PYuLnO7XCR7 -update -skipcrccheck -numListstatusThreads 10  s3a://mlops-hw3-vos/raw_data/ hdfs://rc1a-dataproc-m-1k6fa7ly0haz2jb5.mdb.yandexcloud.net/user/root/datasets/set02/"""

print("get_data")
get_data = BashOperator(
    task_id="get_data",
    bash_command=_bash_command,
    dag=dag_spark,
    run_as_user='Otus_user'
                )


# print('spark submit task to hdfs')
# spark_submit = SparkSubmitOperator(
#     task_id='spark_submit_task_to_hdfs', 
#     application ='/home/ubuntu/utils/clean_data.py' ,
#     conn_id = 'spark_local', 
#     dag=dag_spark
#     )

clean_data = BashOperator(
    task_id="clean_data",
    bash_command=_bash_command,
    dag=dag_spark,
    run_as_user='Otus_user'
                )

get_data >> clean_data

if __name__ == '__main__ ':
    dag_spark.cli()
