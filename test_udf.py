import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

def dummy_sentiment(text):
    return "Neutral"

def vader_sentiment(text):
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()
    score = analyzer.polarity_scores(text)
    return "Positive" if score['compound'] > 0 else "Negative"

def main():
    spark = SparkSession.builder.appName("TestUDF").master("local[*]").getOrCreate()
    
    # Create dummy dataframe
    data = [("I love this!",), ("I hate this!",), ("It is okay.",)]
    df = spark.createDataFrame(data, ["text"])
    
    print("Testing dummy UDF...")
    try:
        dummy_udf = udf(dummy_sentiment, StringType())
        df.withColumn("sentiment", dummy_udf(col("text"))).show()
        print("Dummy UDF succeeded!")
    except Exception as e:
        print(f"Dummy UDF failed: {e}")
        
    print("\nTesting Vader UDF...")
    try:
        vader_udf = udf(vader_sentiment, StringType())
        df.withColumn("sentiment", vader_udf(col("text"))).show()
        print("Vader UDF succeeded!")
    except Exception as e:
        print(f"Vader UDF failed: {e}")

if __name__ == "__main__":
    main()
