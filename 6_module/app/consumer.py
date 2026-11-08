from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField


KAFKA_BROKERS = (
    "localhost:59094,"
    "localhost:59095,"
    "localhost:59096"
)

SCHEMA_REGISTRY_URL = "http://localhost:8081"

TOPIC = "users"
SUBJECT = "users-value"

CONSUMER_GROUP = "practice-7-consumer-group"


def main():
    schema_registry_client = SchemaRegistryClient(
        {
            "url": SCHEMA_REGISTRY_URL
        }
    )

    latest_schema = schema_registry_client.get_latest_version(
        SUBJECT
    )

    schema_str = latest_schema.schema.schema_str

    json_deserializer = JSONDeserializer(
        schema_str,
        schema_registry_client=schema_registry_client,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([TOPIC])

    print(f"Consumer подписан на {TOPIC}")
    print("Ожидаем сообщение...")

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

            user = json_deserializer(
                msg.value(),
                SerializationContext(
                    msg.topic(),
                    MessageField.VALUE,
                ),
            )

            print("Получено сообщение:")
            print(f"key={msg.key().decode('utf-8')}")
            print(f"value={user}")
            print(f"partition={msg.partition()}")
            print(f"offset={msg.offset()}")

            break

    except KeyboardInterrupt:
        print("Consumer остановлен")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()