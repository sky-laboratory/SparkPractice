from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


spark = (
    SparkSession.builder.appName("Streaming Word Count")
    .master("local[*]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()
)

# READ
lines_df = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", "9999")
    .load()
)


# TRANSFORM
words_df = lines_df.select(expr("explode(split(value, ' ')) as word"))
words_df.createOrReplaceTempView("words")
# words_df = lines_df.select("word").groupBy("word").count()

# sql
counts_df = spark.sql("SELECT word, COUNT(*) as count FROM words GROUP BY word")

# SINK
word_count_query = (
    counts_df.writeStream.format("console")
    # micro batch가 생길때 마다 sink에 쓸텐데 기존에 쓰여져 있던건과 합쳐져서 다시 쓰이는거임
    # 무조건 complete는 아니고 싱크에 맞춰서 결정할것
    .outputMode("complete")
    .option("checkopintLocation", "chk-point-dir")
    .start()
)

print("Listening to localhost:9999")
word_count_query.awaitTermination()
