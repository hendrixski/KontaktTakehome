import argparse
import json
import structlog
import logging
from faker import Faker
from confluent_kafka import Producer, Consumer, KafkaError
import psycopg2
from typing import Optional, TYPE_CHECKING
from psycopg2.extensions import connection

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


    # Consumer to verify records  TODO: wait then add a new cursor
#    consumer = Consumer({
#        'bootstrap.servers': 'localhost:29092',
#        'group.id': 'kontakt_group',
#        'auto.offset.reset': 'earliest'
#    })
#    consumer.subscribe(['kontakt_topic'])
#    msg_count = 0
#    while msg_count < args.num_records:
#        msg = consumer.poll(1.0)
#        if msg is None:
#            continue
#        if msg.error():
#            logger.error("Consumer error", error=msg.error())
#            continue
#        record = json.loads(msg.value().decode('utf-8'))
#        logger.info("Consumed record", record=record)
#        msg_count += 1
#        


#conn.close()
if __name__ == "__main__":
    main()
