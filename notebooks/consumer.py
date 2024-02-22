from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, struct, to_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

if __name__ == "__main__":

    kafka_servers = "rc1a-pehqbtiq6ric2ij9.mdb.yandexcloud.net:9091"
    spark_stream = SparkSession.builder.appName("stream").getOrCreate()
    spark_stream.sparkContext.setLogLevel("ERROR")

    schema_inf = StructType(
        [
            StructField("tranaction_id", IntegerType(), True),
            StructField("tx_datetime", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("terminal_id", IntegerType(), True),
            StructField("tx_amount", DoubleType(), True),
            StructField("tx_time_seconds", IntegerType(), True),
            StructField("tx_time_days", IntegerType(), True),
        ]
    )

    model = RandomForestClassificationModel.load("spark_model.joblib")

    df = (
        spark_stream.readStream.format("kafka")
        .option(
            "kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(
                "mlops", "otus-mlops"
            ),
        )
        .option("kafka.ssl.endpoint.identification.algorithm", "https")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.ssl.ca.location", "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", "clicks")
        .option("startingOffsets", "latest")
        .load()
    )

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    df = df.select(from_json(df.value, schema=schema_inf).alias("value"))
    df = df.selectExpr("value.*")

    feat_ext_pipe = PipelineModel.load("feat_ext_pipe.joblib")

    inf = feat_ext_pipe.transform(df)

    transformed_data = model.transform(inf)

    transformed_data.printSchema()

    transformed_data = transformed_data.select(
        "tranaction_id",
        "tx_datetime",
        "customer_id",
        "terminal_id",
        "tx_amount",
        "tx_time_seconds",
        "tx_time_days",
        "prediction",
    )

    transformed_data.select(to_json(struct("*")).alias("value")).writeStream.format("kafka").option(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(
            "mlops", "otus-mlops"
        ),
    ).option("kafka.ssl.endpoint.identification.algorithm", "https").option(
        "kafka.security.protocol", "SASL_SSL"
    ).option(
        "kafka.sasl.mechanism", "PLAIN"
    ).option(
        "kafka.ssl.ca.location",
        "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
    ).option(
        "kafka.bootstrap.servers", kafka_servers
    ).option(
        "topic", "results"
    ).option(
        "checkpointLocation", "dir"
    ).start().awaitTermination()
