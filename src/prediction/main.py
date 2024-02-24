"""App"""

from io import BytesIO

import boto3
import joblib
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi import Depends


class Transaction(BaseModel):
    tranaction_id: int
    tx_datetime: str
    customer_id: int
    terminal_id: int
    tx_amount: float
    tx_time_seconds: int
    tx_time_days: int

# TODO provide secrets to k8s and then read from bucket
# bucket_name = "mlops-hw3-vos"
# s3 = boto3.resource("s3")
# with BytesIO() as data:
#     s3.Bucket(bucket_name).download_fileobj("baseline_model.pkl", data)
#     data.seek(0)
#     model = joblib.load(data)
model = joblib.load("hw_models/model.pkl")

app = FastAPI()


@app.get("/")
def healthcheck():
    return {"status": "OK"}


@app.get("/predict")
def predict(transaction: Transaction = Depends()):
    inf = [
            int(transaction.terminal_id),
            float(transaction.tx_amount),
            int(transaction.tx_time_seconds),
            int(transaction.tx_time_days),
        ]
    pred = model.predict([inf])
    return {"pred": int(pred)}
