export MLFLOW_TRACKING_URI="http://51.250.64.9:8000"
export MLFLOW_S3_ENDPOINT_URL="https://mlops-hw3-vos.website.yandexcloud.net"

spark-submit \
--jars mlflow-spark-1.27.0.jar \
2_flights_pipeline_solution.py \
--output_artifact "test_model_2"
