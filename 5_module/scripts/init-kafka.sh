#!/bin/bash

set -euo pipefail


# ============================================================
# Kafka Practice 6 initialization
# ============================================================

BOOTSTRAP_SERVER="kafka-0:9092"

ADMIN_CONFIG="/opt/kafka-client-config/admin.properties"

CONSUMER_GROUP="practice-6-consumer-group"


echo "=========================================="
echo " Kafka Practice 6 initialization"
echo "=========================================="


# ============================================================
# 1. Waiting for Kafka
# ============================================================

echo
echo "[1/4] Waiting for Kafka..."

until docker compose exec -T kafka-0 \
  kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --command-config "$ADMIN_CONFIG" \
  --list >/dev/null 2>&1
do
  echo "Kafka is not ready yet..."
  sleep 2
done

echo "Kafka is ready."


# ============================================================
# 2. Creating topics
# ============================================================

echo
echo "[2/4] Creating topics..."

docker compose exec -T kafka-0 \
  kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --command-config "$ADMIN_CONFIG" \
  --create \
  --if-not-exists \
  --topic topic-1 \
  --partitions 3 \
  --replication-factor 3

docker compose exec -T kafka-0 \
  kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --command-config "$ADMIN_CONFIG" \
  --create \
  --if-not-exists \
  --topic topic-2 \
  --partitions 3 \
  --replication-factor 3


# ============================================================
# 3. Creating ACLs
# ============================================================

echo
echo "[3/4] Creating ACLs..."


# ------------------------------------------------------------
# Producer -> topic-1
# ------------------------------------------------------------

docker compose exec -T kafka-0 \
  kafka-acls.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --command-config "$ADMIN_CONFIG" \
  --add \
  --allow-principal User:producer \
  --operation Write \
  --operation Describe \
  --topic topic-1


# ------------------------------------------------------------
# Producer -> topic-2
# ------------------------------------------------------------

docker compose exec -T kafka-0 \
  kafka-acls.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --command-config "$ADMIN_CONFIG" \
  --add \
  --allow-principal User:producer \
  --operation Write \
  --operation Describe \
  --topic topic-2


# ------------------------------------------------------------
# Consumer -> topic-1
# ------------------------------------------------------------

docker compose exec -T kafka-0 \
  kafka-acls.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --command-config "$ADMIN_CONFIG" \
  --add \
  --allow-principal User:consumer \
  --operation Read \
  --operation Describe \
  --topic topic-1


# ------------------------------------------------------------
# Consumer -> consumer group
# ------------------------------------------------------------

docker compose exec -T kafka-0 \
  kafka-acls.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --command-config "$ADMIN_CONFIG" \
  --add \
  --allow-principal User:consumer \
  --operation Read \
  --operation Describe \
  --group "$CONSUMER_GROUP"


# ============================================================
# 4. Result
# ============================================================

echo
echo "[4/4] Current topics:"
echo

docker compose exec -T kafka-0 \
  kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --command-config "$ADMIN_CONFIG" \
  --list


echo
echo "Current ACLs:"
echo

docker compose exec -T kafka-0 \
  kafka-acls.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --command-config "$ADMIN_CONFIG" \
  --list


echo
echo "=========================================="
echo " Kafka initialization completed"
echo "=========================================="