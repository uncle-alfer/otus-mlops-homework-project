from pyspark.sql import SparkSession
import os
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import from_json, col, struct, to_json
import json

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

if __name__ == "__main__":

    kafka_servers = ""
    
    spark_stream = SparkSession.builder \
        .appName("stream") \
        .getOrCreate()

    spark_stream.sparkContext.setLogLevel('ERROR')

    schema_inf = StructType(
    [
        StructField("tranaction_id",IntegerType(),True),
        StructField("tx_datetime",StringType(),True),
        StructField("customer_id",IntegerType(),True),
        StructField("terminal_id",IntegerType(),True),
        StructField("tx_amount",DoubleType(),True),
        StructField("tx_time_seconds",IntegerType(),True),
        StructField("tx_time_days",IntegerType(),True),
        # StructField("tx_fraud",IntegerType(),True),
        # StructField("tx_fraud_scenario",IntegerType(),True),
    ]
)
    model = RandomForestClassificationModel.load("spark_model.joblib")
   
    df = spark_stream \
        .readStream \
        .format("kafka") \
        .option("kafka.sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format("mlops", "otus-mlops")) \
        .option("kafka.ssl.endpoint.identification.algorithm", "https") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.ssl.ca.location", '/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt') \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", "clicks") \
        .option("startingOffsets", "latest") \
        .load()\
        .select(from_json(col("value").cast("string"), schema_inf).alias("parsed_value"))\
        .select(col("parsed_value.*"))
   
    # feat_ext_pipe = PipelineModel.load("feat_ext_pipe.joblib") 
    
    # inf = feat_ext_pipe.transform(df)
    
    # transformed_data = model.transform(inf)
    
    # transformed_data = transformed_data.select(
#         'tranaction_id',
#         'tx_datetime',
#         'customer_id',
#         'terminal_id',
#         'tx_amount',
#         'tx_time_seconds',
#         'tx_time_days',
#         'prediction'
#     )
    
#     transformed_data.select(struct("*").cast("string").alias("value"))\
#         .writeStream \
#         .format("kafka") \
#         .option("kafka.sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format("mlops", "otus-mlops")) \
#         .option("kafka.ssl.endpoint.identification.algorithm", "https") \
#         .option("kafka.security.protocol", "SASL_SSL") \
#         .option("kafka.sasl.mechanism", "PLAIN") \
#         .option("kafka.ssl.ca.location", '/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt') \
#         .option("kafka.bootstrap.servers", kafka_servers) \
#         .option("topic", "results") \
#         .option("checkpointLocation", "dir") \
#         .start().awaitTermination()
    transformed_data = df
    transformed_data.select(struct("*").cast("string").alias("value")) \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .start() \
        .awaitTermination()
    

    # spark_stream.streams.awaitAnyTermination()

    
    
