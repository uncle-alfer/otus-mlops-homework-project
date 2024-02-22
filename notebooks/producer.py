import json
import random
import time
from typing import Dict, NamedTuple

import kafka
from faker import Faker

fake = Faker()


class RecordMetadata(NamedTuple):
    key: int
    topic: str
    partition: int
    offset: int


def main():
    kafka_server = "rc1a-pehqbtiq6ric2ij9.mdb.yandexcloud.net:9091"

    producer = kafka.KafkaProducer(
        bootstrap_servers=kafka_server,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username="mlops",
        sasl_plain_password="otus-mlops",
        ssl_cafile="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
        value_serializer=serialize,
    )

    try:
        while True:
            record_md = send_message(producer, "clicks")
            print(
                f"Msg sent. Key: {record_md.key}, topic: {record_md.topic}, partition:{record_md.partition}, offset:{record_md.offset}"
            )
            time.sleep(10)
    except KeyboardInterrupt:
        print(" KeyboardInterrupted!")
        producer.flush()
        producer.close()


def send_message(producer: kafka.KafkaProducer, topic: str) -> RecordMetadata:
    click = generate_click()
    future = producer.send(
        topic=topic,
        key=str(click["tranaction_id"]).encode("ascii"),
        value=click,
    )

    # Block for 'synchronous' sends
    record_metadata = future.get(timeout=1)
    return RecordMetadata(
        key=click["tranaction_id"],
        topic=record_metadata.topic,
        partition=record_metadata.partition,
        offset=record_metadata.offset,
    )


def generate_click() -> Dict:
    return {
        "tranaction_id": random.randint(0, 1000),
        "tx_datetime": fake.date_time_between(start_date="-1y", end_date="+1y").isoformat(),
        "customer_id": random.randint(0, 10000),
        "terminal_id": random.randint(0, 1000),
        "tx_amount": random.uniform(0, 100000),
        "tx_time_seconds": random.randint(0, 1000000),
        "tx_time_days": random.randint(0, 100),
    }


def serialize(msg: Dict) -> bytes:
    return json.dumps(msg).encode("utf-8")


if __name__ == "__main__":
    main()
