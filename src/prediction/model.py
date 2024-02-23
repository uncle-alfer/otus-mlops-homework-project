"""Fast baseline"""

import os
import joblib

import boto3
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

bucket_name = 'mlops-hw3-vos'
session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

data_cleansed = pd.read_parquet(f"s3://{bucket_name}/data_cleansed.parquet")

df = data_cleansed.sample(frac=0.1, random_state=42)

features = ['terminal_id', 'tx_amount', 'tx_time_seconds', 'tx_time_days']
target = "tx_fraud"

X = df[features]
y = df[target]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.33, random_state = 42)

pipeline = Pipeline([
    ('classifier', RandomForestClassifier(random_state = 42))
])

pipeline.fit(X_train, y_train)

preds = pipeline.predict(X_test)

print(np.mean(preds == y_test))

joblib.dump(pipeline, 'hw_models/model.pkl', compress=True)

s3.upload_file('hw_models/model.pkl', bucket_name, 'baseline_model.pkl')

# model = joblib.load('filename.pkl')