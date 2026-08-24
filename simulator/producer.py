import argparse
import json
import os
import time

from confluent_kafka import Producer

from simulator.event_generator import generate_event


def delivery_report(error, message) -> None:
    """
    Callback appelé par Kafka après une tentative d'envoi.

    - error contient une erreur si l'envoi a échoué.
    - message contient les métadonnées du message si l'envoi a réussi.
    """
    if error is not None:
        print(f"Failed to deliver message: {error}")
        return
    print(
        f"Message delivered to topic = {message.topic()} "
        f"key = {message.key()} "
        f"partition = {message.partition()} "
        f"offset = {message.offset()} "
        f"value = {message.value()}"
    )
    


def build_kafka_producer(bootstrap_servers: str) -> Producer:
    """
    Crée et configure un producer Kafka.

    bootstrap_servers indique comment contacter Kafka :
    - localhost:9092 depuis Windows
    - kafka:29092 depuis un conteneur Docker
    """
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "sentinel-sensor-producer",
            "acks": "all",
        }
    )


def send_event_to_kafka(producer: Producer, topic: str, event: dict) -> None:
    """
    Envoie un événement dans Kafka.

    La key Kafka est sensor_id pour garder les événements
    d'un même capteur dans la même partition.
    """
    key = event["sensor_id"]
    value = json.dumps(event)

    producer.produce(
        topic=topic,
        key=key,
        value=value,
        callback=delivery_report,
    )

    # Permet au client Kafka de traiter les callbacks et événements internes.
    producer.poll(0)

def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--interval", type=float, default=0.0)

    parser.add_argument(
        "--scenario",
        choices=["normal", "anomaly", "mixed"],
        default="normal",
    )

    parser.add_argument(
        "--output",
        choices=["console", "kafka"],
        default="console",
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )

    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "sensor-events"),
    )

    args = parser.parse_args()

    kafka_producer = None

    if args.output == "kafka":
        kafka_producer = build_kafka_producer(args.bootstrap_servers)

    for sequence_no in range(1, args.count + 1):
        event = generate_event(sequence_no, args.scenario)

        if args.output == "console":
            print(json.dumps(event), flush=True)

        elif args.output == "kafka":
            send_event_to_kafka(producer=kafka_producer, topic=args.topic, event=event)

        if args.interval > 0:
            time.sleep(args.interval)

    if kafka_producer is not None:
        kafka_producer.flush()

if __name__ == "__main__":
    main()