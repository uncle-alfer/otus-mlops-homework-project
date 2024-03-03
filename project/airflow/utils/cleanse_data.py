import functools
from io import BytesIO

import boto3
import numpy as np
import pandas as pd
from scipy import stats

bucket_name = "project-mlops-object-storage"


def removed_counter(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        df = kwargs.get("df") or args[0]
        start_size = df.shape[1]
        result = func(*args, **kwargs)
        size = df.shape[1]
        print(f"Было удалено {start_size - size} элементов в процессе выполнения {func.__name__}")
        return result

    return wrapper


def read_raw_data():
    s3 = boto3.resource(service_name="s3", endpoint_url="https://storage.yandexcloud.net")
    bucket = s3.Bucket(bucket_name)
    dfs = []
    for obj in bucket.objects.filter(Prefix="raw_data/train"):
        # key = obj.key
        body = obj.get()["Body"].read()
        df = pd.read_csv(BytesIO(body), encoding="utf8")
        dfs.append(df)
    df = pd.concat(dfs)
    return df


@removed_counter
def remove_missing(df: pd.DataFrame):
    df.dropna(how="any", inplace=True)
    return df


@removed_counter
def remove_duplcates(df):
    df.drop_duplicates(inplace=True)
    return df


@removed_counter
def remove_outliers(df):
    cols_to_check = ["CreditScore", "Age", "Tenure", "Balance", "NumOfProducts", "EstimatedSalary"]
    df = df[(np.abs(stats.zscore(df[cols_to_check])) < 3).all(axis=1)]
    return df


@removed_counter
def cleanse_data(df):
    df = remove_missing(df)
    df = remove_duplcates(df)
    df = remove_outliers(df)
    return df


def save_cleansed_data_to_s3(df):
    df.to_parquet(f"s3a://{bucket_name}/processed_data/data.parquet")


if __name__ == "__main__":
    df = read_raw_data()
    df = cleanse_data(df)
    save_cleansed_data_to_s3(df)
