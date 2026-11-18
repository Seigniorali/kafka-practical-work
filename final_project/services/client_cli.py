import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, Producer


BASE_DIR = Path(__file__).resolve().parent.parent

CA_FILE = (
    BASE_DIR
    / "infra"
    / "kafka"
    / "certs"
    / "ca"
    / "ca.crt"
)

REQUESTS_TOPIC = "client.requests"
RECOMMENDATIONS_TOPIC = "recommendations"

KAFKA_SERVERS = (
    "localhost:61094,"
    "localhost:61095,"
    "localhost:61096"
)


def get_client_password():
    password = os.getenv(
        "CLIENT_SERVICE_PASSWORD"
    )

    if not password:
        raise RuntimeError(
            "CLIENT_SERVICE_PASSWORD "
            "environment variable is not set"
        )

    return password


def delivery_report(err, msg):
    if err is not None:
        print(
            f"[ERROR] Request delivery failed: "
            f"{err}"
        )
        return

    print(
        f"[OK] Request sent "
        f"topic={msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


def create_producer():
    password = get_client_password()

    config = {
        "bootstrap.servers": KAFKA_SERVERS,

        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",

        "sasl.username": "client_service",
        "sasl.password": password,

        "ssl.ca.location": str(CA_FILE),

        "acks": "all",
        "enable.idempotence": True,
    }

    return Producer(config)


def create_consumer():
    password = get_client_password()

    config = {
        "bootstrap.servers": KAFKA_SERVERS,

        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",

        "sasl.username": "client_service",
        "sasl.password": password,

        "ssl.ca.location": str(CA_FILE),

        # Отдельная группа именно для просмотра рекомендаций.
        # Новый UUID гарантирует, что сохранённого offset ещё нет.
        "group.id": f"client-recommendations-{uuid.uuid4()}",

        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    return Consumer(config)


def create_request(
    user_id,
    request_type,
    query=None,
):
    return {
        "request_id": str(
            uuid.uuid4()
        ),
        "user_id": user_id,
        "request_type": request_type,
        "query": query,
        "created_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }


def send_request(
    producer,
    request,
):
    message = json.dumps(
        request,
        ensure_ascii=False,
    )

    producer.produce(
        topic=REQUESTS_TOPIC,
        key=request["user_id"],
        value=message,
        callback=delivery_report,
    )

    producer.flush()


def get_recommendation(user_id):
    consumer = create_consumer()

    consumer.subscribe(
        [RECOMMENDATIONS_TOPIC]
    )

    print()
    print(
        f"[CLIENT] Ищем готовые рекомендации "
        f"для user_id={user_id}..."
    )

    latest_recommendation = None
    empty_polls = 0

    try:
        while empty_polls < 3:
            msg = consumer.poll(3.0)

            if msg is None:
                empty_polls += 1
                continue

            empty_polls = 0

            if msg.error():
                print(
                    f"[KAFKA ERROR] "
                    f"{msg.error()}"
                )
                continue

            message_user_id = (
                msg.key().decode("utf-8")
                if msg.key()
                else None
            )

            if message_user_id != user_id:
                continue

            try:
                recommendation = json.loads(
                    msg.value().decode("utf-8")
                )

            except (
                UnicodeDecodeError,
                json.JSONDecodeError,
            ) as error:
                print(
                    f"[ERROR] Не удалось "
                    f"прочитать рекомендацию: "
                    f"{error}"
                )
                continue

            latest_recommendation = recommendation

    finally:
        consumer.close()

    if latest_recommendation is None:
        print()
        print(
            "[CLIENT] Готовых рекомендаций "
            "для этого пользователя пока нет."
        )
        return

    print_recommendation(
        latest_recommendation
    )


def print_recommendation(
    recommendation,
):
    print()
    print(
        "=== Готовые рекомендации ==="
    )

    print(
        f"User: "
        f"{recommendation.get('user_id')}"
    )

    latest_query = recommendation.get(
        "latest_query"
    )

    if latest_query:
        print(
            f"Последний поиск: "
            f"{latest_query}"
        )

    print()

    products = recommendation.get(
        "recommendations",
        [],
    )

    if not products:
        print(
            "Рекомендованных товаров нет."
        )
        return

    for product in products:
        rank = product.get(
            "rank",
            "-",
        )

        name = product.get(
            "name",
            "Без названия",
        )

        product_id = product.get(
            "product_id",
            "-",
        )

        price = product.get(
            "price_amount",
            "-",
        )

        category = product.get(
            "category",
            "-",
        )

        match_score = product.get(
            "match_score",
            0,
        )

        print(
            f"{rank}. {name}"
        )

        print(
            f"   product_id: "
            f"{product_id}"
        )

        print(
            f"   category: "
            f"{category}"
        )

        print(
            f"   price: "
            f"{price} RUB"
        )

        print(
            f"   match_score: "
            f"{match_score}"
        )

        print()


def main():
    producer = create_producer()

    print()
    print(
        "=== Marketplace Client ==="
    )

    user_id = input(
        "Введите user_id: "
    ).strip()

    if not user_id:
        print(
            "user_id не может быть пустым"
        )
        return

    while True:
        print()
        print("1. Найти товар")
        print(
            "2. Отправить запрос "
            "рекомендаций"
        )
        print(
            "3. Посмотреть готовые "
            "рекомендации"
        )
        print("4. Выход")
        print()

        choice = input(
            "Выберите действие: "
        ).strip()

        if choice == "1":
            query = input(
                "Введите название товара: "
            ).strip()

            if not query:
                print(
                    "Название товара "
                    "не может быть пустым"
                )
                continue

            request = create_request(
                user_id=user_id,
                request_type="search",
                query=query,
            )

            send_request(
                producer,
                request,
            )

        elif choice == "2":
            request = create_request(
                user_id=user_id,
                request_type=(
                    "recommendations"
                ),
            )

            send_request(
                producer,
                request,
            )

            print()
            print(
                "[CLIENT] Запрос "
                "рекомендаций отправлен."
            )

            print(
                "[CLIENT] После обработки "
                "аналитической системой "
                "результат можно посмотреть "
                "через пункт 3."
            )

        elif choice == "3":
            get_recommendation(
                user_id
            )

        elif choice == "4":
            print(
                "Выход."
            )
            break

        else:
            print(
                "Неизвестная команда."
            )


if __name__ == "__main__":
    main()