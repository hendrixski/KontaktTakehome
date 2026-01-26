# Create a pyspark dataframe that reads a stream from Kafka: from a broker on port 29092. Subscribe to the the topic "kontakt-topic". 
from pyspark.sql import SparkSession, DataFrame
import uuid
from pyspark.sql.functions import expr

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
#Then create columns for "name", "DOB", and "favorite_color"
df = df.selectExpr("CAST(value AS STRING) as value") \
        .withColumn("name", expr("split(value, ',')[0]")) \
        .withColumn("DOB", expr("split(value, ',')[1]")) \
        .withColumn("favorite_color", expr("split(value, ',')[2]")) \
        .drop("value")

def find_existing_uuids(batch_df: DataFrame) -> DataFrame:
    #collect distinct "name" and "DOB" tuples from the batch_df
    #construct a custom SQL to query a database table for all "name"
    #read that sql from a database named kontakt_database into a dataframe named cache_df
    #join batch_df with cache_df on the "name" and "DOB" fields and return it

    batch_tuples = batch_df.select("name", "DOB").distinct().collect()
    #batch_tuples.createOrReplaceTempView("batch_tuples")
    tuples_list = [(row['name'], row['DOB']) for row in batch_tuples]
    
    if not tuples_list:
        return batch_df.withColumn("uuid", expr("uuid()")).withColumn("is_new", expr("true"))

    conditions = " OR ".join([f"(name = '{name}' AND DOB = '{dob}')" for name, dob in tuples_list])
    sql_query = f"SELECT name, DOB, uuid FROM kontakt_database WHERE {conditions}"
    #an alternative approach will be to use a "IN" clause
    #key_tuples = ", ".join([f"('{row.name}', '{row.DOB}')" for row in distinct_keys])
    #query_string = f"(SELECT * FROM patient_uuid_cache WHERE (name, \"DOB\") IN ({key_tuples})) as filtered_cache"
    
    cache_df = spark.read.format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/kontakt_database") \
            .option("dbtable", f"({sql_query}) as subquery") \
            .option("user", "your_username") \
            .option("password", "your_password")
    #cache_df.createOrReplaceTempView("cache_table") 
    joined_df = batch_df.join(cache_df, on=["name", "DOB"], how="left")
    return joined_df



# Print the DataFrame contents to the console
query = (
    df.writeStream
    .outputMode("append")
    .format("console")
    .start()
)

query.awaitTermination()
