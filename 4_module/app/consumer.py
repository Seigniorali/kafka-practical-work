import json

from confluent_kafka import Consumer, KafkaError


KAFKA_BROKERS = "localhost:39094,localhost:39095,localhost:39096"

TOPICS = [
    "customers.public.users",
    "customers.public.orders",
]


consumer_config = {
    "bootstrap.servers": KAFKA_BROKERS,
    "group.id": "debezium-python-consumer",
    "auto.offset.reset": "earliest",
}


consumer = Consumer(consumer_config)

consumer.subscribe(TOPICS)


print("Consumer запущен.")
print(f"Слушаем топики: {TOPICS}")


try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue

            print(f"Kafka error: {msg.error()}")
            continue


        # ------------------------------------------------------
        # Kafka key
        # ------------------------------------------------------

        key = None

        if msg.key() is not None:
            key = json.loads(
                msg.key().decode("utf-8")
            )


        # ------------------------------------------------------
        # Tombstone
        # ------------------------------------------------------

        if msg.value() is None:
            print("\n" + "=" * 70)

            print(f"Topic:     {msg.topic()}")
            print(f"Partition: {msg.partition()}")
            print(f"Offset:    {msg.offset()}")
            print(f"Key:       {key}")

            print("Tombstone: value = null")

            continue


        # ------------------------------------------------------
        # Обычное Debezium событие
        # ------------------------------------------------------

        value = json.loads(
            msg.value().decode("utf-8")
        )

        payload = value.get("payload", {})

        operation = payload.get("op")
        before = payload.get("before")
        after = payload.get("after")


        print("\n" + "=" * 70)

        print(f"Topic:     {msg.topic()}")
        print(f"Partition: {msg.partition()}")
        print(f"Offset:    {msg.offset()}")
        print(f"Key:       {key}")

        print(f"Operation: {operation}")

        print("Before:")
        print(
            json.dumps(
                before,
                indent=2,
                ensure_ascii=False,
            )
        )

        print("After:")
        print(
            json.dumps(
                after,
                indent=2,
                ensure_ascii=False,
            )
        )


except KeyboardInterrupt:
    print("\nConsumer остановлен.")


finally:
    consumer.close()