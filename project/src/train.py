# from datetime import datetime
# from io import BytesIO

# import boto3
# import numpy as np
# import pandas as pd
# import xgboost as xgb
# from scipy import stats
# from sklearn import preprocessing
# from sklearn.decomposition import PCA
# from sklearn.model_selection import GridSearchCV, train_test_split
# from sklearn.pipeline import Pipeline
# from sklearn.preprocessing import StandardScaler

# bucket_name = "project-mlops-object-storage"


# def read_processed_data():
#     # TODO provide secrets
#     data_cleansed = pd.read_parquet(f"s3://{bucket_name}/processed_data/data.parquet")
#     return data_cleansed


# def create_pipeline(df):

#     X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=8)
