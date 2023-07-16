from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, expr, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    IntegerType,
    ArrayType,
)


spark = (
    SparkSession.builder.appName("Kafka-Streaming")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
)

schema = StructType(
    [
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("title", StringType()),
    ]
)

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka1:9092")
    .option("subscribe", "fake_people")
    .option("startOffsets", "earliest")
    .load()
)
kafka_df.printSchema()


value_df: DataFrame = kafka_df.select(
    from_json(col("value").cast("string"), schema=schema).alias("value")
)
value_df.createOrReplaceTempView("fake_people")
value_df.printSchema()


# Trigger setting
count_df = spark.sql(
    "SELECT value.title, COUNT(1) count FROM fake_people GROUP BY value.title ORDER BY count DESC LIMIT 10"
)
count_write_query = (
    count_df.writeStream.format("console")
    .outputMode("complete")
    .option("checkpointLocation", "chk-point-dir-json")
    .start()
)

print("Listening to Kafka")
count_write_query.awaitTermination()
