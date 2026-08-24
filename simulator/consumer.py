import argparse
import os
import json
from confluent_kafka import Consumer


REQUIRED_FIELDS = {
    "event_id": str,
    "event_time": str,
    "ingest_source": str,
    "site_id": str,
    "asset_id": str,
    "sensor_id": str,
    "metric_name": str,
    "metric_value": float,
    "unit": str,
    "quality_code": str,
    "sequence_no": int,
    "firmware_version": str,
}

def build_kafka_consumer(bootstrap_servers: str, group_id: str, topics: list) -> Consumer: 
    """
    Crée et configure un consumer Kafka.

    bootstrap_servers indique comment contacter Kafka :
    - localhost:9092 depuis Windows
    - kafka:29092 depuis un conteneur Docker

    group_id est l'identifiant du groupe de consommateurs auquel ce consumer appartient.
    topics est la liste des topics auxquels le consumer doit s'abonner.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(topics)
    return consumer

def deserialize_event(message) -> dict:
    raw_value = message.value()
    json_string = raw_value.decode("utf-8")
    event = json.loads(json_string)
    return event

def validate_event(event: dict) -> bool:
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in event:
            return False
        if not isinstance(event[field], expected_type):
            return False
        if expected_type is str and event[field].strip() == "":
            return False
        
    if "anomaly_type" not in event:
        return False
    if not isinstance(event["anomaly_type"], (str, type(None))):  # noqa: SIM103
        return False

    return True

def receive_event_from_kafka(consumer: Consumer) -> None :
    """
    Reçoit un événement depuis Kafka.
    """

    try:
    
        while 1:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error() is not None:
                print(f"Message error = {message.error()}")
                continue
            print(
                f"Message received from topic = {message.topic()} "
                f"key = {message.key()} "
                f"partition = {message.partition()} "
                f"offset = {message.offset()} "
                f"value = {message.value()}"
            )
            event = deserialize_event(message)
            if validate_event(event):
                print(event)
                consumer.commit(message=message, asynchronous=False)
    
    finally:
        consumer.close()
        

def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )

    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "sensor-events"),
    )
    
    parser.add_argument(
        "--group_id",
        default="sentinel-sensor-consumer"
    )

    args = parser.parse_args()

    kafka_consumer = None

    kafka_consumer = build_kafka_consumer(args.bootstrap_servers, args.group_id, [args.topic])

    receive_event_from_kafka(consumer=kafka_consumer)

if __name__ == "__main__":
    main()