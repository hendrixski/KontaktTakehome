# Create a pyspark dataframe that reads a stream from Kafka: from a broker on port 29092. Subscribe to the the topic "kontakt-topic". 
from pyspark.sql import SparkSession
import uuid

print("!!!STARTING MY SPARK APP!!!")
# Create Spark session
spark = SparkSession.builder.appName("KontactPrototype").getOrCreate()

# Read stream from Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "kontakt-topic")
    .load()
)

# The 'value' column is in binary, so cast it to string
df = df.selectExpr("CAST(value AS STRING) as value")

# Print the DataFrame contents to the console
query = (
    df.writeStream
    .outputMode("append")
    .format("console")
    .start()
)

query.awaitTermination()
