import json
import os
from pathlib import Path

from confluent_kafka import Producer


BASE_DIR = Path(__file__).resolve().parent.parent

CA_FILE = (
    BASE_DIR
    / "infra"
    / "kafka"
    / "certs"
    / "ca"
    / "ca.crt"
)

TOPIC = "products.blocklist"

KAFKA_SERVERS = (
    "localhost:61094,"
    "localhost:61095,"
    "localhost:61096"
)


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Blocklist event delivery failed: {err}")
        return

    print(
        f"[OK] Blocklist event sent "
        f"topic={msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


def create_producer():
    password = os.getenv("FILTER_SERVICE_PASSWORD")

    if not password:
        raise RuntimeError(
            "FILTER_SERVICE_PASSWORD environment variable is not set"
        )

    config = {
        "bootstrap.servers": KAFKA_SERVERS,

        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",

        "sasl.username": "filter_service",
        "sasl.password": password,

        "ssl.ca.location": str(CA_FILE),

        "acks": "all",
        "enable.idempotence": True,
    }

    return Producer(config)


def send_event(producer, product_id, action):
    event = {
        "product_id": product_id,
        "action": action,
    }

    message = json.dumps(
        event,
        ensure_ascii=False,
    )

    producer.produce(
        topic=TOPIC,
        key=product_id,
        value=message,
        callback=delivery_report,
    )

    producer.flush()


def main():
    producer = create_producer()

    while True:
        print()
        print("=== Blocklist CLI ===")
        print()
        print("1. Заблокировать товар")
        print("2. Разблокировать товар")
        print("3. Выход")
        print()

        choice = input("Выберите действие: ").strip()

        if choice == "1":
            product_id = input(
                "Введите product_id: "
            ).strip()

            if not product_id:
                print("product_id не может быть пустым")
                continue

            send_event(
                producer=producer,
                product_id=product_id,
                action="block",
            )

        elif choice == "2":
            product_id = input(
                "Введите product_id: "
            ).strip()

            if not product_id:
                print("product_id не может быть пустым")
                continue

            send_event(
                producer=producer,
                product_id=product_id,
                action="unblock",
            )

        elif choice == "3":
            print("Выход.")
            break

        else:
            print("Неизвестная команда.")


if __name__ == "__main__":
    main()