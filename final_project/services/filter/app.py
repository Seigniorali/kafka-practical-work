import os
import ssl
from pathlib import Path

import faust

from models import BlockEvent, Product


BASE_DIR = Path(__file__).resolve().parent.parent.parent

CA_FILE = (
    BASE_DIR
    / "infra"
    / "kafka"
    / "certs"
    / "ca"
    / "ca.crt"
)

KAFKA_BROKER = (
    "kafka://localhost:61094;"
    "localhost:61095;"
    "localhost:61096"
)

FILTER_PASSWORD = os.getenv("FILTER_SERVICE_PASSWORD")

REQUIRED_PRODUCT_FIELDS = {
    "product_id",
    "name",
    "description",
    "price",
    "category",
    "brand",
    "stock",
    "sku",
    "tags",
    "images",
    "specifications",
    "created_at",
    "updated_at",
    "index",
    "store_id",
}

if not FILTER_PASSWORD:
    raise RuntimeError(
        "FILTER_SERVICE_PASSWORD environment variable is not set"
    )


ssl_context = ssl.create_default_context(
    purpose=ssl.Purpose.SERVER_AUTH,
    cafile=str(CA_FILE),
)

ssl_context.verify_flags &= ~ssl.VERIFY_X509_STRICT


credentials = faust.SASLCredentials(
    username="filter_service",
    password=FILTER_PASSWORD,
    mechanism="PLAIN",
    ssl_context=ssl_context,
)


app = faust.App(
    "filter-service-group",

    broker=KAFKA_BROKER,
    broker_credentials=credentials,

    store="memory://",

    topic_partitions=3,
    topic_replication_factor=3,

    consumer_auto_offset_reset="earliest",

    topic_allow_declare=False,
    topic_disable_leader=True,
)


products_raw_topic = app.topic(
    "products.raw",
    key_type=str,
    value_serializer="json",
)


products_filtered_topic = app.topic(
    "products.filtered",
    key_type=str,
    value_type=Product,
)


blocklist_topic = app.topic(
    "products.blocklist",
    key_type=str,
    value_type=BlockEvent,
)


blocked_products = app.Table(
    "blocked-products",
    default=bool,
    partitions=3,
)

@app.agent(blocklist_topic)
async def update_blocklist(stream):

    async for product_id, event in stream.items():

        if product_id != event.product_id:
            print(
                f"[ERROR] Kafka key={product_id} "
                f"does not match product_id={event.product_id}"
            )
            continue

        if event.action == "block":

            blocked_products[product_id] = True

            print(
                f"[BLOCKLIST] product_id={product_id} "
                f"blocked, state={blocked_products[product_id]}"
)

        elif event.action == "unblock":

            blocked_products[product_id] = False

            print(
                f"[BLOCKLIST] product_id={product_id} "
                f"unblocked, state={blocked_products[product_id]}"
            )


        else:

            print(
                f"[ERROR] Unknown blocklist action: "
                f"{event.action}"
            )


@app.agent(products_raw_topic)
async def filter_products(stream):

    async for product_id, product_data in stream.items():

        if not product_id:
            print(
                "[FILTER] Skipped message without Kafka key"
            )
            continue

        if not isinstance(product_data, dict):
            print(
                f"[FILTER] product_id={product_id} "
                f"skipped: value is not JSON object"
            )
            continue

        message_product_id = product_data.get("product_id")

        if product_id != message_product_id:
            print(
                f"[FILTER] product_id={product_id} "
                f"skipped: Kafka key does not match "
                f"message product_id={message_product_id}"
            )
            continue

        missing_fields = (
            REQUIRED_PRODUCT_FIELDS
            - product_data.keys()
        )

        if missing_fields:
            print(
                f"[FILTER] product_id={product_id} "
                f"skipped: missing fields="
                f"{sorted(missing_fields)}"
            )
            continue

        product = Product(**product_data)

        if blocked_products[product_id]:

            print(
                f"[FILTER] product_id={product_id} "
                f"BLOCKED -> dropped"
            )

            continue

        await products_filtered_topic.send(
            key=product_id,
            value=product,
        )

        print(
            f"[FILTER] product_id={product_id} "
            f"allowed -> products.filtered"
        )


if __name__ == "__main__":
    app.main()