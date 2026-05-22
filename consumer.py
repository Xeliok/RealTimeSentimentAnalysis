import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pandas as pd


@pandas_udf(StringType())
def sentiment_udf(texts: pd.Series) -> pd.Series:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()

    def get_sentiment(text):
        if text is None:
            return "Neutral"
        compound = analyzer.polarity_scores(text)['compound']
        if compound >= 0.05:
            return "Positive"
        elif compound <= -0.05:
            return "Negative"
        else:
            return "Neutral"

    return texts.apply(get_sentiment)


def main():
    spark_version = pyspark.__version__
    kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}"

    print(f"Version de PySpark detectee : {spark_version}")
    print(f"Telechargement du connecteur Kafka : {kafka_package}")

    java_options = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED "
        "--add-opens=java.base/sun.misc=ALL-UNNAMED"
    )

    spark = SparkSession.builder \
        .appName("TwitterSentimentAnalysis") \
        .config("spark.jars.packages", kafka_package) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.driver.extraJavaOptions", java_options) \
        .config("spark.executor.extraJavaOptions", java_options) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    print("Connexion a Kafka et attente des donnees...")

    df_kafka = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "tweets_stream") \
        .option("startingOffsets", "latest") \
        .load()

    df_json = df_kafka.selectExpr("CAST(value AS STRING)")
    df_tweets = df_json.select(from_json(col("value"), schema).alias("data")).select("data.*")

    df_analyzed = df_tweets.withColumn("sentiment", sentiment_udf(col("text")))

    df_aggregated = df_analyzed \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "10 seconds"),
            col("sentiment")
        ).count()

    query = df_aggregated \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()