# Create a pyspark dataframe that reads a stream from Kafka: from a broker on port 29092. Subscribe to the the topic "kontakt-topic". 
from pyspark.sql import SparkSession, DataFrame
import uuid
from pyspark.sql.functions import expr
import logging
import sys


# Create Spark session
spark = SparkSession.builder.appName("KontactPrototype").getOrCreate() 
spark.sparkContext.setLogLevel("INFO")
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("STREAMS")

logger.info("--------------STARTING MY APP!!! ------------")


def getKafkaDF(spark: SparkSession) -> DataFrame:
    # Read stream from Kafka
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "kontakt-topic")
        .load()
    )

    # The 'value' column is in binary, so cast it to string
    # it contains JSON records with fields "name", "DOB", and "favorite_color"
    # create columns for "name", "DOB", and "favorite_color"
    df = df.selectExpr("CAST(value AS STRING) as value") \
            .selectExpr("get_json_object(value, '$.name') as name",
                        "get_json_object(value, '$.DOB') as DOB",
                        "get_json_object(value, '$.favorite_color') as favorite_color") \
            .drop("value")

    return df

def find_existing_uuids(batch_df: DataFrame, batch_id):
    #collect distinct "name" and "DOB" tuples from the batch_df
    #construct a custom SQL to query a database table for all "name"
    #read that sql from a database named kontakt_database into a dataframe named cache_df
    #join batch_df with cache_df on the "name" and "DOB" fields and return it
    if batch_df.isEmpty(): 
        logger.info("==============batch is empty============")
        return

    batch_tuples = batch_df.select("name", "DOB").distinct().collect()

    key_tuples = ", ".join([f"('{row.name}', '{row.DOB}')" for row in batch_tuples])
    query_string = f"(SELECT * FROM patient_uuid_cache WHERE (name, DOB) IN ({key_tuples})) as filtered_cache"
    logger.info("------------- QUERY STRING IS ----------")
    logger.info(query_string)

    cache_df = spark.read.format("jdbc") \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", "jdbc:postgresql://postgres:5432/kontakt_database") \
            .option("dbtable", query_string) \
            .option("user", "kontakt") \
            .option("password", "k0ntakt") \
            .load()


    logger.info("------------- CACHE DF SCHEMA IS ----------")
    cache_df.limit(5).show()
    #logger.info(cache_df.schema)
    logger.info("------------- END CACHE DF SCHEMA IS ----------")



    if not batch_df.isEmpty():
        # 3. Use show() but force it to the Driver's stdout
        # This will appear in your spark-submit terminal
        print(f"--------Batch {batch_id} has {batch_df.count()} rows------------")
        batch_df.show(5)
        sys.stdout.flush()


#    tuples_list = [(row['name'], row['DOB']) for row in batch_tuples]
#    
#    if not tuples_list:
#        return batch_df.withColumn("uuid", expr("uuid()")).withColumn("is_new", expr("true"))
#
#    #conditions = " OR ".join([f"(name = '{name}' AND DOB = '{dob}')" for name, dob in tuples_list])
#    #sql_query = f"SELECT name, DOB, uuid FROM kontakt_database WHERE {conditions}"
#    key_tuples = ", ".join([f"('{row.name}', '{row.DOB}')" for row in batch_tuples])
#    query_string = f"(SELECT * FROM patient_uuid_cache WHERE (name, \"DOB\") IN ({key_tuples})) as filtered_cache"
#    
#    cache_df = spark.read.format("jdbc") \
#            .option("url", "jdbc:postgresql://db:5432/kontakt_database") \
#            .option("dbtable", f"({sql_query}) as subquery") \
#            .option("user", "your_username") \
#            .option("password", "your_password")
#    #cache_df.createOrReplaceTempView("cache_table") 
#    joined_df = batch_df.join(cache_df, on=["name", "DOB"], how="left")
#    return joined_df



def printDF(df: DataFrame):
    df.printSchema()
    df.show(truncate=False)


kafka_df = getKafkaDF(spark)
# Print the DataFrame contents to the console
query = (
    kafka_df.writeStream
    .foreachBatch(find_existing_uuids)
    .outputMode("append")
    .start()
)

query.awaitTermination()
