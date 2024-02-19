import pandas as pd
import numpy as np
from sklearn.metrics import precision_recall_fscore_support, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn import set_config

from scipy.stats  import norm, ttest_ind
from scipy import stats

import shap

import warnings
warnings.simplefilter(action='ignore', category=(FutureWarning, UserWarning))

import matplotlib.pyplot as plt
import seaborn as sns

import boto3
import os
import awswrangler as wr

import joblib
import s3fs

data_file_name = "data_cleansed.parquet"
aws_key = os.environ['AWS_ACCESS_KEY_ID']
aws_secret = os.environ['AWS_SECRET_ACCESS_KEY']
endpoint_url = os.environ['AWS_ENDPOINT_URL']
bucket_name = 'mlops-hw3-vos'
s3 = boto3.resource(
    's3',
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret,
    endpoint_url=endpoint_url,
)
bucket = s3.Bucket(bucket_name)

path_ = f"s3://{bucket_name}/{data_file_name}"

def read_data():
    df = pd.read_parquet(path_)
    return df

def split_data(df):
    df_sample = df.sample(frac=0.1, random_state=1)
    feature_cols = ["terminal_id", "tx_amount", "tx_time_seconds", "tx_time_days"]
    target_col = "tx_fraud"
    
    X = df_sample[feature_cols]
    y = df_sample[target_col].values
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    return X_train, X_test, y_train, y_test

def get_pipeline():
    rf_clf = RandomForestClassifier(
        n_estimators=50,
        max_depth=15,
        random_state=42,
        n_jobs=1,
    )
    
    pipe = Pipeline(steps=[
        ("classifier", rf_clf),
    ])
    return pipe

def fit_and_get_metrics(pipe, X_train, X_test, y_train, y_test)
    pipe.fit(X_train, y_train);
    y_pred = pipe.predict(X_test)
    P, R, F1, _ = precision_recall_fscore_support(y_test, y_pred, average="binary")
    roc_auc = roc_auc_score(y_test, y_pred)
    print(f"P: {P:.2f}, R: {R:.2f}, F1: {F1:.5f}, AUC: {roc_auc:.5f}")
    return pipe, y_pred, roc_auc

# def get_model():
#     fs = s3fs.S3FileSystem()
#     output_file = os.path.join(f"s3://{bucket_name}", "random_search_res_short.joblib")
#     with fs.open(output_file, 'rb') as f:
#         random_search_res = joblib.load(f)
#     return random_search_res

def get_f1_hist(y_test, y_pred):
    np.random.seed(42)
    
    bootstrap_iterations = 100
    
    df1 = pd.DataFrame(data={
        "y_test": y_test,
        "y_pred": y_pred,
    })
    
    scores = pd.DataFrame(data={
        "F1": 0.0, "P": 0.0, "R": 0.0, "AUC": 0.0
    }, index=range(bootstrap_iterations))
    
    for i in range(bootstrap_iterations):
        sample = df1.sample(frac=1.0, replace=True)
        scores.loc[i, "F1"] = f1_score(sample["y_test"], sample["y_pred"])
        scores.loc[i, "P"] = precision_score(sample["y_test"], sample["y_pred"])
        scores.loc[i, "R"] = recall_score(sample["y_test"], sample["y_pred"])
        scores.loc[i, "AUC"] = roc_auc_score(sample["y_test"], sample["y_pred"])
    
    ax = sns.histplot(
        x=scores["F1"] 
    )
    
    ax.axvline(x=F1, color="red", linestyle="dashed", linewidth=2);

    f1_mean = scores["F1"].mean()
    f1_std = scores["F1"].std()
    
    
    f1_low = f1_mean - 3 * f1_std
    f1_upp = f1_mean + 3 * f1_std
    
    
    ax = sns.histplot(
        x=scores["F1"] 
    )
    
    x = np.linspace(f1_low, f1_upp, 100)
    y = norm.pdf(x, loc=f1_mean, scale=f1_std)
    ax.plot(x, y, color="blue")
    ax.axvline(f1_mean, color="red", linestyle='dashed')
    ax.axvline(f1_mean-f1_std, color="green", linestyle='dashed')
    ax.axvline(f1_mean+f1_std, color="green", linestyle='dashed');
    
    plt.gcf()
    plt.savefig("h1.png", transparent=True)
    return scores

def shap_interpret(pipe, X_train):
    shap_values_Tree_tr = shap.TreeExplainer(pipe.named_steps["classifier"]).shap_values(X_train)
    shap.summary_plot(shap_values_Tree_tr, X_train)
    plt.gcf()
    plt.savefig("shap_interpret.png", transparent=True)

df = read_data()
X_train, X_test, y_train, y_test = split_data(df)
pipe = get_pipeline()
pipe, y_pred, roc_auc = fit_and_get_metrics(pipe, X_train, X_test, y_train, y_test)
scores = get_f1_hist(y_test, y_pred)
shap_interpret(pipe, X_train)