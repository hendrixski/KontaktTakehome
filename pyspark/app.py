# Create a pyspark dataframe that reads a stream from Kafka: from a broker on port 29092. Subscribe to the the topic "kontakt-topic". 
from pyspark.sql import SparkSession, DataFrame
import uuid
import sys
from pyspark.sql import functions as F


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

    joined_df = batch_df.join(cache_df, on=["name", "DOB"], how="left")

    logger.info("------------- CACHE DF SCHEMA IS ----------")
    cache_df.limit(10).show()

    logger.info("------------- MERGED DF SCHEMA IS ----------")
    joined_df.limit(10).show()


    #create a dataframe with only the rows from joined_df where the uuid is null
    #these are the new rows that need to be assigned a uuid
    new_rows_df = joined_df.filter(joined_df.uuid.isNull())
    old_rows_df = joined_df.filter(joined_df.uuid.isNotNull())
    #new_rows_df.limit(10).show()

    #for each row in new_rows_df, generate a new uuid and add it to the row
    def assign_uuid(row):
        return row.asDict() | {"uuid": str(uuid.uuid4())}
    #new_rows_with_uuid_df = new_rows_df.rdd.map(assign_uuid).toDF()
    new_rows_with_uuid_df = new_rows_df.withColumn("uuid", F.expr("uuid()"))

    #write the new rows with uuids back to the patient_uuid_cache table in the database
    new_rows_with_uuid_df.write \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", "jdbc:postgresql://postgres:5432/kontakt_database") \
            .option("dbtable", "patient_uuid_cache") \
            .option("user", "kontakt") \
            .option("password", "k0ntakt") \
            .mode("append") \


    #write the uuid and favorite_color fields from df to another table in the same database named anonymized_patients
    new_rows_with_uuid_df.write \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", "jdbc:postgresql://postgres:5432/kontakt_database") \
            .option("dbtable", "anonymized_patients") \
            .option("user", "kontakt") \
            .option("password", "k0ntakt") \
            .mode("append") \
            
    #use the old_rows_df to overwrite the anonymized_patients table
    old_rows_df.select("uuid", "favorite_color").write \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", "jdbc:postgresql://postgres:5432/kontakt_database") \
        .option("dbtablename", "anonymized_patients") \
        .option("user", "kontakt") \
        .option("password", "k0ntakt") \
        .mode("overwrite") 

    if not batch_df.isEmpty():
        # show the df and force stdout to write everything by flushing stdout
        logger.info(f"--------Batch {batch_id} has {batch_df.count()} rows------------")
        batch_df.show(5)
        sys.stdout.flush()


kafka_df = getKafkaDF(spark)
# Print the DataFrame contents to the console
query = (
    kafka_df.writeStream
    .foreachBatch(find_existing_uuids)
    .outputMode("append")
    .start()
)

query.awaitTermination()
