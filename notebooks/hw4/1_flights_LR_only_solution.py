import os
import logging
import argparse

from sklearn.datasets import load_diabetes
from pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

import mlflow


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def get_dataframe():
    db = load_diabetes()
    df = DataFrame(data=db.data, columns=db.feature_names)

    return df


def get_regression():
    regression = LinearRegression(featuresCol='features', labelCol='bp')
    return regression


def main(args):
    os.environ["AWS_ACCESS_KEY_ID"] = ...
    os.environ["AWS_SECRET_ACCESS_KEY"] = ...
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"

    logger.info("========================tracking URI: %s", {mlflow.get_tracking_uri()})

    mlflow.set_experiment("pyspark_experiment")

    logger.info("========================Creating Spark Session ...")
    spark = SparkSession\
        .builder\
        .appName("pyspark_experiment_1")\
        .getOrCreate()

    logger.info("========================Loading Data ...")
    data = spark.createDataFrame(get_dataframe())
    
    assembler = VectorAssembler(inputCols=["age", "bmi"], outputCol="features")
    train_data = assembler.transform(data)
    
    regression = get_regression()

    model = regression.fit(train_data)

    logger.info("========================Saving model ...")
    mlflow.spark.log_model(model, artifact_path=args.output_artifact)
    mlflow.spark.save_model(model, args.output_artifact)

    spark.stop()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Model (Inference Pipeline) Training")

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name for the output serialized model (Inference Artifact folder)",
        required=True,
    )

    args = parser.parse_args()

    main(args)



