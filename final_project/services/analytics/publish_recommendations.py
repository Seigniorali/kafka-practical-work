import json
import os

import requests
from confluent_kafka import Producer


HDFS_URL = os.getenv(
    "HDFS_URL",
    "http://hdfs-namenode:9870",
)

RECOMMENDATIONS_DIR = (
    "/marketplace/recommendations"
)

PRIMARY_KAFKA_SERVERS = os.getenv(
    "PRIMARY_KAFKA_SERVERS",
    (
        "primary-kafka-0:9092,"
        "primary-kafka-1:9092,"
        "primary-kafka-2:9092"
    ),
)

CA_FILE = os.getenv(
    "CA_FILE",
    "/app/ca.crt",
)

ANALYTICS_PASSWORD = os.getenv(
    "ANALYTICS_SERVICE_PASSWORD"
)

TOPIC = "recommendations"


if not ANALYTICS_PASSWORD:
    raise RuntimeError(
        "ANALYTICS_SERVICE_PASSWORD "
        "environment variable is not set"
    )


def create_producer():
    config = {
        "bootstrap.servers": PRIMARY_KAFKA_SERVERS,

        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",

        "sasl.username": "analytics_service",
        "sasl.password": ANALYTICS_PASSWORD,

        "ssl.ca.location": CA_FILE,

        "acks": "all",
        "enable.idempotence": True,
    }

    return Producer(config)


def list_recommendation_files():
    url = (
        f"{HDFS_URL}/webhdfs/v1"
        f"{RECOMMENDATIONS_DIR}"
    )

    response = requests.get(
        url,
        params={
            "op": "LISTSTATUS",
        },
        timeout=30,
    )

    response.raise_for_status()

    data = response.json()

    files = []

    for item in data["FileStatuses"]["FileStatus"]:

        filename = item["pathSuffix"]

        if (
            filename.startswith("part-")
            and filename.endswith(".json")
        ):
            files.append(filename)

    return files


def read_hdfs_file(filename):
    path = (
        f"{RECOMMENDATIONS_DIR}/"
        f"{filename}"
    )

    url = (
        f"{HDFS_URL}/webhdfs/v1"
        f"{path}"
    )

    response = requests.get(
        url,
        params={
            "op": "OPEN",
        },
        allow_redirects=True,
        timeout=30,
    )

    response.raise_for_status()

    return response.text


def delivery_report(err, msg):

    if err is not None:
        print(
            f"[ERROR] Delivery failed: {err}"
        )
        return

    print(
        f"[OK] Recommendation sent "
        f"topic={msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


def main():
    producer = create_producer()

    files = list_recommendation_files()

    if not files:
        print(
            "[INFO] No recommendation "
            "files found in HDFS"
        )
        return

    print(
        f"[INFO] Found HDFS files: {files}"
    )

    sent_count = 0

    for filename in files:

        content = read_hdfs_file(
            filename
        )

        for line in content.splitlines():

            if not line.strip():
                continue

            recommendation = json.loads(
                line
            )

            user_id = recommendation.get(
                "user_id"
            )

            if not user_id:
                print(
                    "[SKIP] Recommendation "
                    "without user_id"
                )
                continue

            producer.produce(
                topic=TOPIC,
                key=user_id.encode("utf-8"),
                value=json.dumps(
                    recommendation,
                    ensure_ascii=False,
                ).encode("utf-8"),
                callback=delivery_report,
            )

            producer.poll(0)

            sent_count += 1

            print(
                f"[SEND] user_id={user_id} "
                f"request_id="
                f"{recommendation.get('request_id')}"
            )

    producer.flush()

    print(
        f"[DONE] Sent recommendations: "
        f"{sent_count}"
    )


if __name__ == "__main__":
    main()