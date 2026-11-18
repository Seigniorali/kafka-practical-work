#!/usr/bin/env bash

set -e

CERT_ROOT="infra/kafka/certs"
CA_DIR="${CERT_ROOT}/ca"

BROKERS=(
  "primary-kafka-0"
  "primary-kafka-1"
  "primary-kafka-2"
)

for BROKER in "${BROKERS[@]}"; do

  echo "========================================"
  echo "Generating certificate for ${BROKER}"
  echo "========================================"

  BROKER_DIR="${CERT_ROOT}/${BROKER}"

  mkdir -p "${BROKER_DIR}"

  # 1. Private key
  openssl genpkey \
    -algorithm RSA \
    -out "${BROKER_DIR}/kafka.keystore.key" \
    -pkeyopt rsa_keygen_bits:2048

  # 2. Certificate Signing Request
  openssl req \
    -new \
    -key "${BROKER_DIR}/kafka.keystore.key" \
    -out "${BROKER_DIR}/${BROKER}.csr" \
    -subj "/C=KZ/O=Marketplace Final Project/CN=${BROKER}"

  # 3. SAN configuration
  cat > "${BROKER_DIR}/san.cnf" <<EOF
subjectAltName=DNS:${BROKER},DNS:localhost,IP:127.0.0.1
EOF

  # 4. Sign broker certificate with our CA
  openssl x509 \
    -req \
    -in "${BROKER_DIR}/${BROKER}.csr" \
    -CA "${CA_DIR}/ca.crt" \
    -CAkey "${CA_DIR}/ca.key" \
    -CAcreateserial \
    -out "${BROKER_DIR}/kafka.keystore.pem" \
    -days 3650 \
    -sha256 \
    -extfile "${BROKER_DIR}/san.cnf"

  # 5. Broker trusts our CA
  cp \
    "${CA_DIR}/ca.crt" \
    "${BROKER_DIR}/kafka.truststore.pem"

  # Temporary files are no longer needed
  rm \
    "${BROKER_DIR}/${BROKER}.csr" \
    "${BROKER_DIR}/san.cnf"

  chmod 600 "${BROKER_DIR}/kafka.keystore.key"

  echo "${BROKER}: OK"
done

echo
echo "All Kafka certificates generated successfully."