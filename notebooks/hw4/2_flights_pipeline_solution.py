import os
import logging
import argparse
from datetime import datetime

from sklearn.datasets import load_diabetes
from pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression

from pyspark.ml import Pipeline

import mlflow
from mlflow.tracking import MlflowClient


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def get_dataframe():
    db = load_diabetes()
    df = DataFrame(data=db.data, columns=db.feature_names)

    return df


def get_pipeline():
    # indexer = ...
    # onehot = ...
    # assembler = ...
    regression = LinearRegression(featuresCol='features', labelCol='bp')
    
    # pipeline = ...

    # return pipeline
    pass


def main(args):
    
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
    TRACKING_SERVER_HOST = "127.0.0.1"
    mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:5000")
    logger.info("tracking URI: %s", {mlflow.get_tracking_uri()})

    logger.info("Creating Spark Session ...")
    spark = SparkSession\
        .builder\
        .appName("pyspark_experiment_2")\
        .getOrCreate()

    logger.info("Loading Data ...")
    data = spark.createDataFrame(get_dataframe())

    # Prepare MLFlow experiment for logging
    client = MlflowClient()
    experiment = client.get_experiment_by_name("pyspark_experiment")
    experiment_id = experiment.experiment_id
    
    # Добавьте в название вашего run имя, по которому его можно будет найти в MLFlow
    run_name = 'My run name' + ' ' + str(datetime.now())

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
        
        inf_pipeline = get_pipeline()

        logger.info("Fitting new model / inference pipeline ...")
        model = inf_pipeline.fit(data)
        
        logger.info("Scoring the model ...")
        # evaluator = ...
        predictions = model.transform(data)
        # rmse = evaluator.evaluate(predictions)
                    
        run_id = mlflow.active_run().info.run_id
        logger.info(f"Logging metrics to MLflow run {run_id} ...")
        mlflow.log_metric("rmse", rmse)
        logger.info(f"Model RMSE: {rmse}")

        logger.info("Saving model ...")
        mlflow.spark.save_model(model, args.output_artifact)

        logger.info("Exporting/logging model ...")
        mlflow.spark.log_model(model, args.output_artifact)
        logger.info("Done")

    spark.stop()
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Model (Inference Pipeline) Training")

    # При запуске используйте оригинальное имя 'Student_Name_flights_LR_only'
    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name for the output serialized model (Inference Artifact folder)",
        required=True,
    )

    args = parser.parse_args()

    main(args)


