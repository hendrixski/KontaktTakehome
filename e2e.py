import argparse
import json
import structlog
import logging
from faker import Faker
from confluent_kafka import Producer
import psycopg2
from psycopg2.extensions import connection
from time import sleep

conn: connection = psycopg2.connect(
    dbname="kontakt_database",
    user="kontakt",
    password="k0ntakt",
    host="localhost",
    port="5432"
)


def configure_logger():
    logging.basicConfig(
        filename="kontakt.log",
        filemode="a",
        level=logging.DEBUG,
        format="%(message)s"
    )
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
    )
    logging.getLogger('faker').setLevel(logging.ERROR)
    return structlog.get_logger()

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")

def main():
    parser = argparse.ArgumentParser(description="Generate and send fake records to Kafka.")
    parser.add_argument('-n', '--num-records', type=int, default=10, help='Number of records to generate (default: 10)')
    args = parser.parse_args()

    logger = configure_logger()

    producer = Producer({'bootstrap.servers': 'localhost:29092'})
    fake = Faker()

    test_records = []
    try:
        cursor = conn.cursor()
        for _ in range(args.num_records):
            record = {
                "name": fake.name(),
                "DOB": fake.date_of_birth(minimum_age=0, maximum_age=90).isoformat(),
                "favorite_color": fake.color_name()
            }
            logger.debug("Generated record", record=record)

            if(fake.boolean(chance_of_getting_true=50)): #randomly write about half of them to the DB
                #connect to a postgresql database and write the "name" and "DOB" fields to a table named patient_uuid_cache
                new_uuid = fake.uuid4()
                insert_query = f"""
                    INSERT INTO patient_uuid_cache (name, DOB, uuid)
                    VALUES ('{record["name"]}', '{record["DOB"]}', '{new_uuid}')
                    ON CONFLICT (name, DOB) DO NOTHING;
                """
                cursor.execute(insert_query)
                logger.debug(f"Inserted record into database name={record['name']}, DOB={record['DOB']}, uuid={new_uuid}")
            else:
                test_records.append(record)
            conn.commit()

            #send record to Kafka topic "kontakt-topic"
            producer.produce(
                topic="kontakt-topic",
                value=json.dumps(record).encode("utf-8"),
                callback=delivery_report
            )

        cursor.close()
        producer.flush()
        logger.info("All records sent", count=args.num_records)

    except Exception as e:
        logger.error("Database insertion error. Unable to conduct test. Terminating", error=str(e))
        exit(1)


    if(len(test_records) > 0):
        #pick the "name" and "dob" values from the first record from test_records 
        test_first_name = test_records[0]["name"]
        test_dob = test_records[0]["DOB"]
        #wait 5 seconds then open a new cursor using connection conn 
        #then query database table patient_uuid_cache for the existence of a record with name=test_first_name and dob=test_dob
        wait_seconds = 5
        sleep(wait_seconds)
        cursor = conn.cursor()
        select_query = f"""
            SELECT uuid FROM patient_uuid_cache
            WHERE name = '{test_first_name}' AND DOB = '{test_dob}';
        """
        cursor.execute(select_query)
        result = cursor.fetchone()
        if result:
            logger.info("SUCCESS! Test record found in database", name=test_first_name, DOB=test_dob, uuid=result[0])
        else:
            logger.error("FAIL! Test record NOT found in database", name=test_first_name, DOB=test_dob)
        

#conn.close()
if __name__ == "__main__":
    main()
