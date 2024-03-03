import logging
import os
from datetime import datetime

import pandas as pd
import xgboost as xgb
from mlflow.tracking import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

import mlflow

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


bucket_name = "project-mlops-object-storage"

categorical_cols = [
    "Geography",
    "Gender",
    "HasCrCard",
    "IsActiveMember",
]
numerical_cols = [
    "CreditScore",
    "Age",
    "Tenure",
    "Balance",
    "NumOfProducts",
    "EstimatedSalary",
]
features = categorical_cols + numerical_cols
target = "Exited"


def read_processed_data():
    data_cleansed = pd.read_parquet(f"s3a://{bucket_name}/processed_data/data.parquet")
    return data_cleansed


def get_pipeline():
    numerical_transformer = SimpleImputer(strategy="constant")

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numerical_transformer, numerical_cols),
            ("cat", categorical_transformer, categorical_cols),
        ]
    )

    model = xgb.XGBClassifier()

    pipeline = Pipeline(steps=[("preprocessor", preprocessor), ("model", model)])

    return pipeline


def split_data(df):
    X = df[features]
    y = df[target]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)
    return X_train, X_test, y_train, y_test


def fit_pipeline(pipeline, df):
    os.environ["MLFLOW_TRACKING_URI"] = "http://158.160.97.90:8000"
    logger.info("tracking URI: %s", {mlflow.get_tracking_uri()})
    param_grid = {
        # 'model__max_depth': [2, 3, 5, 7, 10],
        # 'model__n_estimators': [10, 100, 500],
        "model__max_depth": [10],
        "model__n_estimators": [100],
    }
    grid = GridSearchCV(pipeline, param_grid, cv=5, n_jobs=-1, scoring="roc_auc")
    X_train, X_test, y_train, y_test = split_data(df)
    # Prepare MLFlow experiment for logging
    client = MlflowClient()

    experiment_name = "bank-churn1"
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        mlflow.create_experiment(
            experiment_name, artifact_location="s3://project-mlops-object-storage/mlflow/artifacts"
        )
        mlflow.set_experiment(experiment_name)
        experiment = client.get_experiment_by_name(experiment_name)
    experiment_id = experiment.experiment_id

    run_name = "bank-churn-run" + " " + str(datetime.now())
    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
        grid.fit(X_train, y_train)
        # Log params, metrics and model with MLFlow

        run_id = mlflow.active_run().info.run_id
        logger.info(f"Logging optimal parameters to MLflow run {run_id} ...")

        best_estimator = grid.best_estimator_
        best_params = grid.best_params_

        mlflow.log_param("best_estimator", best_estimator)
        mlflow.log_param("best_params", best_params)

        logger.info("Scoring the model ...")
        y_pred = grid.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        logger.info(f"Logging metrics to MLflow run {run_id} ...")
        mlflow.log_metric("accuracy", accuracy)
        logger.info(f"Model accuracy: {accuracy}")

        logger.info("Saving pipeline ...")
        # mlflow.sklearn.save_model(grid, f"inf_clf-{run_name}")
        # mlflow.sklearn.save_model(grid, "inf_clf")

        logger.info("Exporting/logging pipline ...")
        # mlflow.sklearn.log_model(grid, f"inf_clf-{run_name}")
        mlflow.sklearn.log_model(grid, "inf_clf")
        logger.info("Done")

    return grid


# def main():
#     df = read_processed_data()
#     pipeline = get_pipeline()
#     X_train, X_test, y_train, y_test = split_data(df)
#     model = fit_pipeline(pipeline, X_train, y_train)
#     # model to s3

if __name__ == "__main__":
    # main()
    df = read_processed_data()
    pipeline = get_pipeline()
    # X_train, X_test, y_train, y_test = split_data(df)
    model = fit_pipeline(pipeline, df)
    # y_pred = model.predict(X_test)
    # print(np.mean(y_pred == y_test))

    # import boto3
    # s3 = boto3.resource('s3')
    # my_bucket = s3.Bucket(bucket_name)
    # for my_bucket_object in my_bucket.objects.all():
    #     print(my_bucket_object)
