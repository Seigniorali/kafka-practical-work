import sys
from pathlib import Path

from confluent_kafka import Consumer, KafkaError


# ---------------------------------------------------------
# Kafka
# ---------------------------------------------------------

KAFKA_BROKERS = (
    "localhost:49094,"
    "localhost:49095,"
    "localhost:49096"
)

CONSUMER_GROUP = "practice-6-consumer-group"


# ---------------------------------------------------------
# Путь до Root CA
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

CA_PATH = BASE_DIR / "certs" / "ca" / "ca.crt"


# ---------------------------------------------------------
# Consumer configuration
# ---------------------------------------------------------

CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BROKERS,

    "security.protocol": "SASL_SSL",

    "sasl.mechanism": "PLAIN",

    "sasl.username": "consumer",

    "sasl.password": "consumer-secret",

    "ssl.ca.location": str(CA_PATH),

    "ssl.endpoint.identification.algorithm": "https",

    "group.id": CONSUMER_GROUP,

    "auto.offset.reset": "earliest",

    "enable.auto.commit": True,
}


def main():
    if len(sys.argv) != 2:
        print("Использование:")
        print("python app/consumer.py <topic>")
        print()
        print("Например:")
        print("python app/consumer.py topic-1")
        return

    topic = sys.argv[1]

    consumer = Consumer(CONSUMER_CONFIG)

    consumer.subscribe([topic])

    print(f"Consumer подписан на {topic}")
    print("Ожидаем сообщения...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue

                print(f"Kafka error: {msg.error()}")
                break

            value = msg.value().decode("utf-8")

            print(
                "Получено сообщение:"
                f" topic={msg.topic()},"
                f" partition={msg.partition()},"
                f" offset={msg.offset()},"
                f" value={value}"
            )

            break

    except KeyboardInterrupt:
        print("Consumer остановлен")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()