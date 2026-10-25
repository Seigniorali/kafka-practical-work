# Kafka Practical Work 6 — Kafka Security with KRaft, TLS, SASL and ACL

## Описание

Практическая работа посвящена настройке защищённого Apache Kafka-кластера из трёх узлов

В рамках работы реализованы:

- Kafka-кластер из 3 broker/controller узлов;
- KRaft вместо ZooKeeper;
- SSL/TLS для шифрования соединений;
- отдельный сертификат для каждого Kafka broker;
- JKS Keystore и Truststore для каждого broker;
- SASL/PLAIN для аутентификации пользователей;
- SASL_SSL для защищённого взаимодействия;
- StandardAuthorizer для работы ACL в KRaft;
- разграничение доступа Producer и Consumer;
- автоматическое создание topics и ACL через `init-kafka.sh`;
- Python Producer и Consumer на `confluent-kafka`

---

## Архитектура

Кластер состоит из трёх Kafka-узлов:

```text
kafka-0
Broker + Controller

kafka-1
Broker + Controller

kafka-2
Broker + Controller
```

Используется combined KRaft mode:

```text
                    KRaft Cluster

          ┌─────────────┼─────────────┐
          │             │             │
          ▼             ▼             ▼

       kafka-0        kafka-1        kafka-2
   Broker+Controller Broker+Controller Broker+Controller
```

### Listeners

На каждом Kafka broker используются три listener:

```text
INTERNAL    :9092
CONTROLLER  :9093
EXTERNAL    :9094
```

Назначение:

```text
INTERNAL
Broker ↔ Broker

CONTROLLER
KRaft Controller ↔ Controller

EXTERNAL
Python Client ↔ Kafka
```

Все listener используют:

```text
SASL_SSL
```

То есть соединения одновременно используют:

```text
TLS
→ шифрование соединения

SASL
→ аутентификация пользователя

ACL
→ авторизация операций
```

---

## Порты

Внешние Kafka-порты:

```text
kafka-0 → localhost:49094
kafka-1 → localhost:49095
kafka-2 → localhost:49096
```

Python Producer и Consumer используют все три адреса как bootstrap servers

---

## Пользователи

Настроены три SASL/PLAIN пользователя:

```text
admin
producer
consumer
```

### Admin

Используется для административных операций:

- создание topics;
- создание ACL;
- просмотр topics;
- просмотр ACL.

`User:admin` настроен как Kafka superuser

### Producer

Principal:

```text
User:producer
```

Разрешено:

```text
topic-1
WRITE
DESCRIBE

topic-2
WRITE
DESCRIBE
```

### Consumer

Principal:

```text
User:consumer
```

Разрешено:

```text
topic-1
READ
DESCRIBE
```

Также разрешена работа с consumer group:

```text
practice-6-consumer-group
READ
DESCRIBE
```

Доступ на чтение `topic-2` для Consumer отсутствует

---

## ACL

Итоговая модель доступа:

| Principal | Resource | READ | WRITE | DESCRIBE |
|---|---|---:|---:|---:|
| `User:producer` | `topic-1` | ❌ | ✅ | ✅ |
| `User:producer` | `topic-2` | ❌ | ✅ | ✅ |
| `User:consumer` | `topic-1` | ✅ | ❌ | ✅ |
| `User:consumer` | `topic-2` | ❌ | ❌ | ❌ |
| `User:consumer` | `practice-6-consumer-group` | ✅ | — | ✅ |

Kafka настроена с:

```text
allow.everyone.if.no.acl.found=false
```

Поэтому отсутствие разрешающего ACL означает запрет доступа

Для `topic-2` Consumer не имеет `READ`, поэтому получает:

```text
TOPIC_AUTHORIZATION_FAILED
```

---

## TLS и сертификаты

Создан собственный Root CA:

```text
certs/ca/ca.crt
certs/ca/ca.key
```

Для каждого Kafka broker создан отдельный сертификат

Пример для `kafka-0`:

```text
certs/kafka-0/
├── kafka-0.cnf
├── kafka-0.key
├── kafka-0.csr
├── kafka-0.crt
├── kafka-0.keystore.p12
├── kafka-0.keystore.jks
└── kafka-0.truststore.jks
```

Аналогичная структура используется для:

```text
kafka-1
kafka-2
```

### Keystore

Keystore содержит:

```text
Private Key
+
Broker Certificate
+
Root CA certificate
```

Назначение:

```text
Keystore = КТО Я
```

### Truststore

Truststore содержит доверенный Root CA.

Назначение:

```text
Truststore = КОМУ Я ДОВЕРЯЮ
```

---

## Структура проекта

```text
5_module/
│
├── app/
│   ├── producer.py
│   └── consumer.py
│
├── certs/
│   ├── ca/
│   ├── kafka-0/
│   ├── kafka-1/
│   └── kafka-2/
│
├── config/
│   ├── admin.properties
│   ├── producer.properties
│   └── consumer.properties
│
├── scripts/
│   └── init-kafka.sh
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# Запуск проекта

## 1. Создать Python virtual environment

Пример:

```bash
python3 -m venv .venv
```

Активировать:

```bash
source .venv/bin/activate
```

---

## 2. Установить Python dependencies

```bash
pip install -r requirements.txt
```

Основная библиотека:

```text
confluent-kafka
```

---

## 3. Запустить Kafka cluster

```bash
docker compose up -d
```

Проверить:

```bash
docker compose ps
```

Должны работать:

```text
practice-6-kafka-0
practice-6-kafka-1
practice-6-kafka-2
```

---

## 4. Инициализировать Kafka

Скрипт автоматически:

- ожидает готовности Kafka;
- создаёт `topic-1`;
- создаёт `topic-2`;
- создаёт ACL для Producer;
- создаёт ACL для Consumer;
- создаёт ACL для Consumer Group;
- выводит итоговый список topics и ACL

Запуск:

```bash
./scripts/init-kafka.sh
```

Если нет права на выполнение:

```bash
chmod +x scripts/init-kafka.sh
```

---

# Проверка Producer

## Отправка в topic-1

```bash
python app/producer.py topic-1
```

Ожидаемый результат:

```text
Отправляем сообщение в topic-1:
Message from Python producer to topic-1

Сообщение доставлено:
topic=topic-1
```

---

## Отправка в topic-2

```bash
python app/producer.py topic-2
```

Ожидаемый результат:

```text
Отправляем сообщение в topic-2:
Message from Python producer to topic-2

Сообщение доставлено:
topic=topic-2
```

Producer имеет право записывать сообщения в оба topic

---

# Проверка Consumer

## Чтение topic-1

```bash
python app/consumer.py topic-1
```

Ожидаемый результат:

```text
Consumer подписан на topic-1
Ожидаем сообщения...

Получено сообщение:
value=Message from Python producer to topic-1
```

Consumer имеет `READ` ACL для `topic-1`.

---

## Попытка чтения topic-2

```bash
python app/consumer.py topic-2
```

Ожидаемый результат:

```text
TOPIC_AUTHORIZATION_FAILED
```

или:

```text
Broker: Topic authorization failed
```

Это ожидаемый результат

Consumer успешно прошёл:

```text
TLS
SASL authentication
```

но Kafka StandardAuthorizer запретил операцию, так как:

```text
User:consumer
```

не имеет:

```text
READ
```

для:

```text
topic-2
```

---

# Проверка воспроизводимости

Проект был протестирован после полного удаления Kafka containers и volumes

Удаление:

```bash
docker compose down -v
```

После этого выполнялись только:

```bash
docker compose up -d
```

и:

```bash
./scripts/init-kafka.sh
```

Скрипт успешно восстановил:

```text
topic-1
topic-2
ACL Producer
ACL Consumer
ACL Consumer Group
```

После чистого запуска были повторно проверены Python-клиенты:

```text
Producer → topic-1  ✅
Producer → topic-2  ✅

Consumer → topic-1  ✅
Consumer → topic-2  ❌ TOPIC_AUTHORIZATION_FAILED
```

Таким образом проект воспроизводится с чистых Docker volumes

---

# Security Flow

Полный путь подключения клиента:

```text
Python Client
      │
      ▼
SASL_SSL
      │
      ▼
TLS Handshake
      │
      ├── Broker Certificate
      │
      └── Root CA Verification
      │
      ▼
SASL/PLAIN
      │
      ▼
Authentication
      │
      ▼
User:producer
или
User:consumer
      │
      ▼
StandardAuthorizer
      │
      ▼
ACL
      │
      ▼
ALLOW / DENY
```

---

## Важно

Сертификаты, private keys и пароли в данном проекте созданы исключительно для учебной лабораторной работы

Они не должны использоваться в production-среде

В production:

- private keys не должны храниться в публичном Git-репозитории;
- passwords должны храниться в Secret Manager / Vault;
- нельзя использовать демонстрационные credentials;
- Root CA private key должен храниться отдельно и защищённо