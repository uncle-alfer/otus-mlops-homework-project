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


bucket_name = "mlops-hw3-vos"
s3 = boto3.resource("s3")
with BytesIO() as data:
    s3.Bucket(bucket_name).download_fileobj("baseline_model.pkl", data)
    data.seek(0)
    model = joblib.load(data)

app = FastAPI()


@app.get("/")
def healthcheck():
    return {"status": "OK"}


@app.get("/predict")
def predict(transaction: Transaction = Depends()):
    pred = model.predict(
        [
            int(transaction.terminal_id),
            float(transaction.tx_amount),
            int(transaction.tx_time_seconds),
            int(transaction.tx_time_days),
        ]
    )

    return {"pred": pred}


# inf = {
#     "tranaction_id": 123664,
#     "tx_datetime": "2022-11-06 14:21:07",
#     "customer_id": 573306,
#     "terminal_id": 844,
#     "tx_amount": 28.64,
#     "tx_time_seconds": 101312467,
#     "tx_time_days": 1172,
# }
# inf_ = list(inf.values())
# inf_ = [
#     v
#     for k, v in inf.items()
#     if k in ["terminal_id", "tx_amount", "tx_time_seconds", "tx_time_days"]
# ]
# print(inf_)
# print(model.predict([inf_]))
