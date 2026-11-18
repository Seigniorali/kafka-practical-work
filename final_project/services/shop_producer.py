import json
import os
from pathlib import Path

from confluent_kafka import Producer


BASE_DIR = Path(__file__).resolve().parent.parent

PRODUCTS_FILE = BASE_DIR / "data" / "products.json"

CA_FILE = BASE_DIR / "infra" / "kafka" / "certs" / "ca" / "ca.crt"

TOPIC = "products.raw"

KAFKA_SERVERS = (
    "localhost:61094,"
    "localhost:61095,"
    "localhost:61096"
)


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Message delivery failed: {err}")
        return

    print(
        f"[OK] product sent "
        f"topic={msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


def load_products():
    with open(PRODUCTS_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def create_producer():
    password = os.getenv("SHOP_PRODUCER_PASSWORD")

    if not password:
        raise RuntimeError(
            "SHOP_PRODUCER_PASSWORD environment variable is not set"
        )

    config = {
        "bootstrap.servers": KAFKA_SERVERS,

        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",

        "sasl.username": "shop_producer",
        "sasl.password": password,

        "ssl.ca.location": str(CA_FILE),

        "acks": "all",
        "enable.idempotence": True,
    }

    return Producer(config)


def main():
    products = load_products()

    print(f"Loaded products: {len(products)}")

    producer = create_producer()

    for product in products:
        product_id = product["product_id"]

        message = json.dumps(
            product,
            ensure_ascii=False,
        )

        print(
            f"Sending product "
            f"id={product_id}, "
            f"name={product['name']}"
        )

        producer.produce(
            topic=TOPIC,
            key=product_id,
            value=message,
            callback=delivery_report,
        )

        producer.poll(0)

    producer.flush()

    print("All products sent.")


if __name__ == "__main__":
    main()