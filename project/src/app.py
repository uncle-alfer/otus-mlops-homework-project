"""App"""

# from io import BytesIO

# import boto3
import os
import logging
import pandas as pd
from fastapi import Depends, FastAPI
from pydantic import BaseModel
from starlette_exporter import PrometheusMiddleware, handle_metrics
from prometheus_client import Counter

from mlflow.tracking import MlflowClient

import mlflow

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net/"
os.environ["MLFLOW_TRACKING_URI"] = "http://158.160.97.90:8000"
client = MlflowClient()
model_version = client.get_model_version_by_alias(name="inf1", alias="champion")
# TODO provide secrets to k8s and then read from bucket
model = mlflow.pyfunc.load_model(model_version.source)

bucket_name = "project-mlops-object-storage"

class Сustomer(BaseModel):
    credit_score: int
    geography: str
    gender: str
    age: float
    tenure: int
    balance: float
    num_of_products: int
    has_cr_card: float
    is_active_member: float
    estimated_salary: float


app = FastAPI()
app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", handle_metrics)

ZERO_COUNTER = Counter("zero_answered", "zero_answered")
ALL_COUNTER = Counter("all_counter", "all_counter")


@app.get("/")
def healthcheck():
    return {"status": "OK"}


@app.get("/predict")
def predict(customer: Сustomer = Depends()):
    inf = [
        int(customer.credit_score),
        str(customer.geography),
        str(customer.gender),
        float(customer.age),
        int(customer.tenure),
        float(customer.balance),
        int(customer.num_of_products),
        float(customer.has_cr_card),
        float(customer.is_active_member),
        float(customer.estimated_salary),
    ]
    features = ["CreditScore", "Geography", "Gender", "Age", "Tenure", "Balance", "NumOfProducts", "HasCrCard", "IsActiveMember", "EstimatedSalary",]
    df = pd.DataFrame([inf], columns=features)

    pred = model.predict(df)
    ans = int(pred)
    if ans == 0:
        ZERO_COUNTER.inc()
    ALL_COUNTER.inc()
    return {"pred": ans}
