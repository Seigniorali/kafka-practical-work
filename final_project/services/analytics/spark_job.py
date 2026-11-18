import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


PRODUCTS_PATH = os.getenv(
    "PRODUCTS_PATH",
    "hdfs://hdfs-namenode:9000/marketplace/products/*.json",
)

REQUESTS_PATH = os.getenv(
    "REQUESTS_PATH",
    "hdfs://hdfs-namenode:9000/marketplace/client_requests/*.json",
)

RECOMMENDATIONS_PATH = os.getenv(
    "RECOMMENDATIONS_PATH",
    "hdfs://hdfs-namenode:9000/marketplace/recommendations",
)


def main():
    spark = (
        SparkSession.builder
        .appName("marketplace-recommendations")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("[SPARK] Reading products from HDFS")

    products = spark.read.json(
        PRODUCTS_PATH
    )

    print("[SPARK] Reading client requests from HDFS")

    requests = spark.read.json(
        REQUESTS_PATH
    )

    print("[SPARK] Products:")
    products.show(
        truncate=False
    )

    print("[SPARK] Client requests:")
    requests.show(
        truncate=False
    )

    # В HDFS могут находиться несколько версий
    # одного и того же товара после повторных запусков.
    #
    # Для каждого product_id оставляем только
    # самую свежую запись по updated_at.
    product_window = (
        Window
        .partitionBy("product_id")
        .orderBy(
            F.col("updated_at").desc()
        )
    )

    unique_products = (
        products
        .withColumn(
            "product_row_number",
            F.row_number().over(
                product_window
            ),
        )
        .filter(
            F.col("product_row_number") == 1
        )
        .drop(
            "product_row_number"
        )
    )

    print("[SPARK] Unique products:")
    unique_products.show(
        truncate=False
    )

    products_for_recommendation = (
        unique_products
        .select(
            "product_id",
            "name",
            "category",
            "brand",
            F.col("price.amount").alias(
                "price_amount"
            ),
            F.col("stock.available").alias(
                "stock_available"
            ),
        )
    )

    # Для каждого пользователя ищем
    # его самый последний search-запрос.
    search_window = (
        Window
        .partitionBy("user_id")
        .orderBy(
            F.col("created_at").desc()
        )
    )

    latest_search = (
        requests
        .filter(
            F.col("request_type") == "search"
        )
        .filter(
            F.col("query").isNotNull()
        )
        .withColumn(
            "row_number",
            F.row_number().over(
                search_window
            ),
        )
        .filter(
            F.col("row_number") == 1
        )
        .select(
            "user_id",
            F.col("query").alias(
                "latest_query"
            ),
        )
    )

    # Для каждого пользователя берём
    # последний запрос recommendations.
    recommendation_window = (
        Window
        .partitionBy("user_id")
        .orderBy(
            F.col("created_at").desc()
        )
    )

    recommendation_requests = (
        requests
        .filter(
            F.col("request_type")
            == "recommendations"
        )
        .withColumn(
            "row_number",
            F.row_number().over(
                recommendation_window
            ),
        )
        .filter(
            F.col("row_number") == 1
        )
        .select(
            "request_id",
            "user_id",
        )
    )

    # Для каждого recommendation request
    # создаём набор кандидатов из доступных товаров.
    candidates = (
        recommendation_requests
        .crossJoin(
            products_for_recommendation
        )
        .join(
            latest_search,
            on="user_id",
            how="left",
        )
    )

    # Если название товара содержит
    # последний поисковый запрос пользователя,
    # даём такому товару высокий score.
    candidates = (
        candidates
        .withColumn(
            "match_score",
            F.when(
                F.col("latest_query").isNotNull()
                & F.expr(
                    """
                    lower(name)
                    LIKE concat(
                        '%',
                        lower(latest_query),
                        '%'
                    )
                    """
                ),
                F.lit(100),
            ).otherwise(
                F.lit(0)
            ),
        )
    )

    # Ранжирование:
    # 1. Совпадение с поисковым запросом.
    # 2. Количество товара на складе.
    # 3. Цена.
    ranking_window = (
        Window
        .partitionBy("request_id")
        .orderBy(
            F.col("match_score").desc(),
            F.col("stock_available").desc(),
            F.col("price_amount").asc(),
        )
    )

    ranked = (
        candidates
        .withColumn(
            "rank",
            F.row_number().over(
                ranking_window
            ),
        )
        .filter(
            F.col("rank") <= 3
        )
    )

    # Собираем товары обратно
    # в один recommendations-массив.
    recommendations = (
        ranked
        .groupBy(
            "request_id",
            "user_id",
            "latest_query",
        )
        .agg(
            F.sort_array(
                F.collect_list(
                    F.struct(
                        "rank",
                        "product_id",
                        "name",
                        "category",
                        "brand",
                        "price_amount",
                        "stock_available",
                        "match_score",
                    )
                )
            ).alias(
                "recommendations"
            )
        )
        .withColumn(
            "generated_at",
            F.current_timestamp(),
        )
    )

    print(
        "[SPARK] Generated recommendations:"
    )

    recommendations.show(
        truncate=False
    )

    (
        recommendations
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(
            RECOMMENDATIONS_PATH
        )
    )

    print(
        "[SPARK] Recommendations written "
        "to HDFS"
    )

    spark.stop()


if __name__ == "__main__":
    main()