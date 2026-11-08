from confluent_kafka import Consumer


KAFKA_BROKERS = (
    "localhost:59094,"
    "localhost:59095,"
    "localhost:59096"
)

TOPIC = "nifi-topic"


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": "nifi-consumer-group",
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([TOPIC])

    print(f"Consumer подписан на {TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            value = msg.value().decode("utf-8")

            print(
                f"Получено сообщение: {value}, "
                f"partition={msg.partition()}, "
                f"offset={msg.offset()}"
            )

    except KeyboardInterrupt:
        print("\nConsumer остановлен")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()