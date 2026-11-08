from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import SerializationContext, MessageField


KAFKA_BROKERS = (
    "localhost:59094,"
    "localhost:59095,"
    "localhost:59096"
)

SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "users"

BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = BASE_DIR / "schema" / "user.json"


def delivery_report(err, msg):
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
    schema_str = SCHEMA_PATH.read_text(encoding="utf-8")

    schema_registry_client = SchemaRegistryClient(
        {
            "url": SCHEMA_REGISTRY_URL
        }
    )

    json_serializer = JSONSerializer(
        schema_str,
        schema_registry_client,
        conf={
            "auto.register.schemas": False
        }
    )

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "acks": "all",
        }
    )

    user = {
        "name": "Ali",
        "favoriteNumber": 42,
        "favoriteColor": "blue",
    }

    print("Отправляем пользователя:")
    print(user)

    serialized_value = json_serializer(
        user,
        SerializationContext(
            TOPIC,
            MessageField.VALUE,
        ),
    )

    producer.produce(
        topic=TOPIC,
        key="user-1",
        value=serialized_value,
        callback=delivery_report,
    )

    producer.flush()


if __name__ == "__main__":
    main()