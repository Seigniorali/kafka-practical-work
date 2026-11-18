#!/usr/bin/env bash

set -e

if [ ! -f ".env" ]; then
  echo ".env not found"
  exit 1
fi

set -a
source .env
set +a

CLIENT_DIR="infra/kafka/client"

mkdir -p "${CLIENT_DIR}"

create_client_config() {
  local file_name="$1"
  local username="$2"
  local password="$3"

  cat > "${CLIENT_DIR}/${file_name}.properties" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${username}" password="${password}";

ssl.truststore.type=PEM
ssl.truststore.location=/opt/bitnami/kafka/config/certs/kafka.truststore.pem

ssl.endpoint.identification.algorithm=https
EOF
}

create_client_config \
  "shop-producer" \
  "shop_producer" \
  "${SHOP_PRODUCER_PASSWORD}"

create_client_config \
  "filter-service" \
  "filter_service" \
  "${FILTER_SERVICE_PASSWORD}"

create_client_config \
  "client-service" \
  "client_service" \
  "${CLIENT_SERVICE_PASSWORD}"

create_client_config \
  "mirror-maker" \
  "mirror_maker" \
  "${MIRROR_MAKER_PASSWORD}"

create_client_config \
  "connect-service" \
  "connect_service" \
  "${CONNECT_SERVICE_PASSWORD}"

create_client_config \
  "analytics-service" \
  "analytics_service" \
  "${ANALYTICS_SERVICE_PASSWORD}"

chmod 600 "${CLIENT_DIR}"/*.properties

echo "Client properties generated."