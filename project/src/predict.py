import logging
import os
from io import BytesIO

import boto3
import pandas as pd
from mlflow.tracking import MlflowClient

import mlflow

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net/"
os.environ["MLFLOW_TRACKING_URI"] = "http://158.160.97.90:8000"
client = MlflowClient()
model_version = client.get_model_version_by_alias(name="inf1", alias="champion")

model = mlflow.pyfunc.load_model(model_version.source)

bucket_name = "project-mlops-object-storage"


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


df = read_raw_data()

print(model.predict(df))
# print(model)
