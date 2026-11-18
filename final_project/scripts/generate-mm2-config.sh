#!/usr/bin/env bash

set -e

if [ ! -f ".env" ]; then
  echo ".env not found"
  exit 1
fi

set -a
source .env
set +a

cat > infra/mm2/mm2.properties <<EOF
# =========================================
# CLUSTERS
# =========================================

clusters = primary, secondary

primary.bootstrap.servers = primary-kafka-0:9092,primary-kafka-1:9092,primary-kafka-2:9092
secondary.bootstrap.servers = secondary-kafka-0:9092,secondary-kafka-1:9092,secondary-kafka-2:9092


# =========================================
# PRIMARY SECURITY
# =========================================

primary.security.protocol = SASL_SSL
primary.sasl.mechanism = PLAIN
primary.sasl.jaas.config = org.apache.kafka.common.security.plain.PlainLoginModule required username="mirror_maker" password="${MIRROR_MAKER_PASSWORD}";

primary.ssl.truststore.type = PEM
primary.ssl.truststore.location = /opt/mm2/ca.crt
primary.ssl.endpoint.identification.algorithm = https


# =========================================
# SECONDARY SECURITY
# =========================================

secondary.security.protocol = SASL_SSL
secondary.sasl.mechanism = PLAIN
secondary.sasl.jaas.config = org.apache.kafka.common.security.plain.PlainLoginModule required username="mirror_maker" password="${MIRROR_MAKER_PASSWORD}";

secondary.ssl.truststore.type = PEM
secondary.ssl.truststore.location = /opt/mm2/ca.crt
secondary.ssl.endpoint.identification.algorithm = https


# =========================================
# REPLICATION FLOW
# =========================================

primary->secondary.enabled = true
secondary->primary.enabled = false

primary->secondary.topics = products.raw,products.filtered,products.blocklist,client.requests,recommendations

tasks.max = 3

refresh.topics.enabled = true
refresh.topics.interval.seconds = 10


# =========================================
# TOPIC SETTINGS
# =========================================

replication.factor = 3

sync.topic.configs.enabled = true
sync.topic.acls.enabled = false


# =========================================
# MM2 INTERNAL TOPICS
# =========================================

checkpoints.topic.replication.factor = 3
heartbeats.topic.replication.factor = 3
offset-syncs.topic.replication.factor = 3

offset.storage.replication.factor = 3
status.storage.replication.factor = 3
config.storage.replication.factor = 3


# Store offset-sync information on target.
# This lets the source side remain mostly read-only.

offset-syncs.topic.location = target


# We do not need consumer-group failover for this coursework.

emit.checkpoints.enabled = false

emit.heartbeats.enabled = true
emit.heartbeats.interval.seconds = 5
EOF

echo "MirrorMaker 2 configuration generated."