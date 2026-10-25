import sys
from pathlib import Path

from confluent_kafka import Producer


# ---------------------------------------------------------
# Kafka
# ---------------------------------------------------------

KAFKA_BROKERS = (
    "localhost:49094,"
    "localhost:49095,"
    "localhost:49096"
)


# ---------------------------------------------------------
# Путь до Root CA
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

CA_PATH = BASE_DIR / "certs" / "ca" / "ca.crt"


# ---------------------------------------------------------
# Producer configuration
# ---------------------------------------------------------

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BROKERS,

    "security.protocol": "SASL_SSL",

    "sasl.mechanism": "PLAIN",

    "sasl.username": "producer",

    "sasl.password": "producer-secret",

    "ssl.ca.location": str(CA_PATH),

    "ssl.endpoint.identification.algorithm": "https",
}


def delivery_report(err, msg):
    """
    Callback вызывается Kafka после попытки доставки сообщения.
    """

    if err is not None:
        print(f"Ошибка доставки: {err}")
        return

    print(
        "Сообщение доставлено:"
        f" topic={msg.topic()},"
        f" partition={msg.partition()},"
        f" offset={msg.offset()}"
    )


def main():
    if len(sys.argv) != 2:
        print("Использование:")
        print("python app/producer.py <topic>")
        print()
        print("Например:")
        print("python app/producer.py topic-1")
        return

    topic = sys.argv[1]

    producer = Producer(PRODUCER_CONFIG)

    message = f"Message from Python producer to {topic}"

    print(f"Отправляем сообщение в {topic}:")
    print(message)

    producer.produce(
        topic=topic,
        value=message.encode("utf-8"),
        callback=delivery_report,
    )

    producer.flush()


if __name__ == "__main__":
    main()