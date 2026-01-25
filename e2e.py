import argparse
import json
import structlog
import logging
from faker import Faker
from confluent_kafka import Producer, Consumer, KafkaError

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

    for _ in range(args.num_records):
        record = {
            "name": fake.name(),
            "DOB": fake.date_of_birth(minimum_age=0, maximum_age=90).isoformat(),
            "favorite_color": fake.color_name()
        }
        logger.debug("Generated record", record=record)
        producer.produce(
            topic="kontakt_topic",
            value=json.dumps(record).encode("utf-8"),
            callback=delivery_report
        )

    producer.flush()
    logger.info("All records sent", count=args.num_records)

    # Consumer to verify records
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

if __name__ == "__main__":
    main()
