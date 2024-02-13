# ! pip install findspark

from datetime import datetime

import findspark
import pandas as pd
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col,isnan,when,count, trim


findspark.init()
findspark.find()


def setup_session():
    spark = (
        SparkSession
            .builder
            .appName("OTUS")
            .config("spark.executor.memory", "10g")
            .config("spark.driver.memory", "10g")
            .getOrCreate()
    )
    return spark

def read_raw_data():
    schema = StructType(
        [
            StructField("tranaction_id",IntegerType(),True),
            StructField("tx_datetime",StringType(),True),
            StructField("customer_id",IntegerType(),True),
            StructField("terminal_id",IntegerType(),True),
            StructField("tx_amount",DoubleType(),True),
            StructField("tx_time_seconds",IntegerType(),True),
            StructField("tx_time_days",IntegerType(),True),
            StructField("tx_fraud",IntegerType(),True),
            StructField("tx_fraud_scenario",IntegerType(),True),
        ]
    )

    df = spark.read.options(header=True, delimiter=",", inferSchema=False).schema(schema).csv(
        '/user/root/datasets/set02'
    )
    return df

def remove_missing(df_):
    start_size = df_.count()
    df_ = df_.dropna(how="any")
    size = df_.count()
    print(f"Было удалено {start_size - size} элементов как пропусков")
    return df_


def remove_duplcates(df_):
    start_size = df_.count()
    df_ = df_.dropDuplicates()
    size = df_.count()
    print(f"Было удалено {start_size - size} элементов как дубликатов")
    return df_


def remove_outliers(df_, col_name):
    start_size = df_.count()
    Q1 = df_.approxQuantile(col_name, [0.25], 0.05)[0]
    Q3 = df_.approxQuantile(col_name, [0.75], 0.05)[0]
    IQR = Q3 - Q1
    ub = Q3 + 1.5 * IQR
    df_ = df_.filter(F.col(col_name) <= ub)
    size = df_.count()
    print(f"Для колонки {col_name} было удалено {start_size - size} элементов как выбросов")
    return df_


def remove_incorrect_date(df_):
    start_size = df_.count()
    date_pattern = "^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
    df_ = df_.filter(F.col("tx_datetime").rlike(date_pattern))
    size = df_.count()
    print(f"Было удалено {start_size - size} с датой в неправильном формате")
    return df_


def cleanse_data(df_):
    start_size = df_.count()
    df_ = remove_duplcates(df_)
    df_ = remove_incorrect_date(df_)
    df_ = remove_missing(df_)
    for col_name in ["tx_amount", "terminal_id", "tx_time_seconds", "tx_time_days"]:
        df_ = remove_outliers(df_, col_name)
    size = df_.count()
    print(f"После очистки из {start_size} строк осталось {size}")
    return df_

def save_cleansed_data_to_s3(df_):
    df_.write.parquet(f"s3a://mlops-hw3-vos/data_cleansed_{datetime.now()}.parquet",mode="overwrite")
    

    
df = read_raw_data()
df = cleanse_data()
save_cleansed_data_to_s3(df)
