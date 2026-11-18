import os
from urllib.parse import quote

import requests
from confluent_kafka import Consumer


KAFKA_SERVERS = os.getenv(
    "KAFKA_SERVERS",
    (
        "secondary-kafka-0:9092,"
        "secondary-kafka-1:9092,"
        "secondary-kafka-2:9092"
    ),
)

HDFS_URL = os.getenv(
    "HDFS_URL",
    "http://hdfs-namenode:9870",
)

CA_FILE = os.getenv(
    "CA_FILE",
    "/app/ca.crt",
)

ANALYTICS_PASSWORD = os.getenv(
    "ANALYTICS_SERVICE_PASSWORD"
)

if not ANALYTICS_PASSWORD:
    raise RuntimeError(
        "ANALYTICS_SERVICE_PASSWORD environment variable is not set"
    )


TOPIC_TO_HDFS_DIR = {
    "primary.products.filtered": "/marketplace/products",
    "primary.client.requests": "/marketplace/client_requests",
}


def create_consumer():
    config = {
        "bootstrap.servers": KAFKA_SERVERS,

        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",

        "sasl.username": "analytics_service",
        "sasl.password": ANALYTICS_PASSWORD,

        "ssl.ca.location": CA_FILE,

        "group.id": "analytics-hdfs-group",

        "auto.offset.reset": "earliest",

        "enable.auto.commit": False,
    }

    return Consumer(config)


def write_to_hdfs(
    hdfs_path,
    content,
):
    encoded_path = quote(
        hdfs_path,
        safe="/",
    )

    url = (
        f"{HDFS_URL}"
        f"/webhdfs/v1"
        f"{encoded_path}"
    )

    response = requests.put(
        url,
        params={
            "op": "CREATE",
            "overwrite": "true",
        },
        data=content.encode("utf-8"),
        headers={
            "Content-Type": "application/json",
        },
        allow_redirects=True,
        timeout=30,
    )

    response.raise_for_status()


def build_hdfs_path(msg):
    hdfs_directory = TOPIC_TO_HDFS_DIR[
        msg.topic()
    ]

    topic_name = msg.topic().replace(
        ".",
        "_",
    )

    filename = (
        f"{topic_name}"
        f"-p{msg.partition()}"
        f"-o{msg.offset()}"
        f".json"
    )

    return (
        f"{hdfs_directory}/"
        f"{filename}"
    )


def main():
    consumer = create_consumer()

    topics = list(
        TOPIC_TO_HDFS_DIR.keys()
    )

    consumer.subscribe(topics)

    print(
        f"[HDFS] Listening topics: "
        f"{topics}"
    )

    try:

        while True:

            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(
                    f"[KAFKA ERROR] "
                    f"{msg.error()}"
                )
                continue

            value = msg.value()

            if value is None:
                print(
                    f"[SKIP] Empty value "
                    f"topic={msg.topic()} "
                    f"partition={msg.partition()} "
                    f"offset={msg.offset()}"
                )

                consumer.commit(
                    message=msg,
                    asynchronous=False,
                )

                continue

            content = value.decode(
                "utf-8"
            )

            hdfs_path = build_hdfs_path(
                msg
            )

            try:

                write_to_hdfs(
                    hdfs_path,
                    content,
                )

            except Exception as error:

                print(
                    f"[HDFS ERROR] "
                    f"path={hdfs_path} "
                    f"error={error}"
                )

                continue

            consumer.commit(
                message=msg,
                asynchronous=False,
            )

            print(
                f"[OK] "
                f"topic={msg.topic()} "
                f"partition={msg.partition()} "
                f"offset={msg.offset()} "
                f"-> {hdfs_path}"
            )

    except KeyboardInterrupt:

        print(
            "\nStopping HDFS consumer..."
        )

    finally:

        consumer.close()


if __name__ == "__main__":
    main()