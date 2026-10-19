# Practical Work 5 — PostgreSQL CDC with Debezium, Kafka Connect, Prometheus and Grafana

## Цель работы

Настроить потоковую передачу изменений из PostgreSQL в Apache Kafka с использованием Debezium и Kafka Connect.

В рамках практической работы реализовано:

- Apache Kafka cluster из 3 brokers в режиме KRaft;
- PostgreSQL с таблицами `users` и `orders`;
- logical replication PostgreSQL;
- Kafka Connect в Distributed Mode;
- Debezium PostgreSQL Source Connector;
- фильтрация CDC только для таблиц `users` и `orders`;
- обработка `INSERT`, `UPDATE`, `DELETE`;
- Python consumer для чтения CDC-событий из Kafka;
- JMX Exporter для метрик Kafka Connect;
- Prometheus для сбора метрик;
- Grafana для визуализации состояния и производительности Kafka Connect;
- Kafka UI для просмотра Kafka topics.

---

# 1. Архитектура

```text
                        PostgreSQL
                            │
                            │ WAL
                            ▼
                    Logical Replication
                            │
                            ▼
                Debezium PostgreSQL Connector
                            │
                            ▼
                    Kafka Connect Worker
                            │
                  ┌─────────┴──────────┐
                  │                    │
                  ▼                    ▼
         Apache Kafka              JMX Metrics
                  │                    │
        ┌─────────┴────────┐           ▼
        │                  │      JMX Exporter
        ▼                  ▼           │
customers.public.users  customers.     ▼
                        public.orders Prometheus
        │                              │
        └──── Python Consumer          ▼
                                     Grafana
```

Основной поток CDC:

```text
INSERT / UPDATE / DELETE
          │
          ▼
      PostgreSQL
          │
          ▼
         WAL
          │
          ▼
Replication Slot
          │
          ▼
Debezium
          │
          ▼
Kafka Connect
          │
          ▼
Apache Kafka
          │
          ▼
Python Consumer
```

---

# 2. Структура проекта

```text
4_module/
├── app/
│   └── consumer.py
│
├── connector/
│   └── debezium-postgres.json
│
├── kafka-connect/
│   ├── Dockerfile
│   ├── jmx-config.yml
│   └── jmx_prometheus_javaagent-0.15.0.jar
│
├── postgres/
│   ├── custom-config.conf
│   └── init.sql
│
├── prometheus/
│   └── prometheus.yml
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

Назначение основных файлов:

| Файл | Назначение |
|---|---|
| `docker-compose.yml` | Запуск всей инфраструктуры |
| `postgres/init.sql` | Создание таблиц и первоначальных данных |
| `postgres/custom-config.conf` | Настройка PostgreSQL logical WAL |
| `connector/debezium-postgres.json` | Конфигурация Debezium Connector |
| `app/consumer.py` | Python consumer CDC-событий |
| `kafka-connect/Dockerfile` | Кастомный Kafka Connect image с JMX Exporter |
| `kafka-connect/jmx-config.yml` | Настройка JMX Exporter |
| `prometheus/prometheus.yml` | Настройка Prometheus |
| `requirements.txt` | Python-зависимости |

---

# 3. Компоненты

## Apache Kafka

Используется кластер из трёх Kafka brokers в режиме KRaft:

```text
kafka-0
kafka-1
kafka-2
```

Внешние порты:

```text
kafka-0 → localhost:39094
kafka-1 → localhost:39095
kafka-2 → localhost:39096
```

Внутри Docker network Kafka Connect использует:

```text
kafka-0:9092
kafka-1:9092
kafka-2:9092
```

---

## PostgreSQL

Используется:

```text
postgres:16
```

Подключение с хоста:

```text
localhost:35432
```

Подключение внутри Docker:

```text
postgres:5432
```

База данных:

```text
customers
```

Пользователь:

```text
postgres-user
```

---

## Kafka Connect + Debezium

Kafka Connect доступен через REST API:

```text
http://localhost:38083
```

Используется Debezium PostgreSQL Connector:

```text
io.debezium.connector.postgresql.PostgresConnector
```

В ходе проверки использовалась версия Debezium plugin:

```text
3.2.4.Final
```

Connector:

```text
pg-connector
```

Работает одна Task:

```text
Task 0
```

---

## Kafka UI

Kafka UI доступен по адресу:

```text
http://localhost:38080
```

Через интерфейс можно просматривать:

- brokers;
- topics;
- partitions;
- messages;
- consumer groups;
- Kafka Connect.

---

## JMX Exporter

Метрики Kafka Connect экспортируются через:

```text
http://localhost:39876/metrics
```

Внутри Docker:

```text
kafka-connect:9876
```

---

## Prometheus

Prometheus доступен по адресу:

```text
http://localhost:39090
```

Он собирает метрики с:

```text
kafka-connect:9876
```

---

## Grafana

Grafana доступна по адресу:

```text
http://localhost:33000
```

Данные для локального входа:

```text
login:    admin
password: admin
```

---

# 4. Настройка PostgreSQL

Для работы Debezium PostgreSQL должен поддерживать logical replication.

Файл:

```text
postgres/custom-config.conf
```

Содержит:

```conf
listen_addresses = '*'
wal_level = 'logical'
max_wal_senders = 1
max_replication_slots = 1
```

### `wal_level = logical`

Позволяет читать изменения PostgreSQL через logical decoding.

Поток выглядит следующим образом:

```text
SQL operation
     │
     ▼
PostgreSQL
     │
     ▼
WAL
     │
     ▼
Debezium
```

### `max_wal_senders = 1`

Разрешает один процесс передачи WAL.

### `max_replication_slots = 1`

Разрешает создание одного replication slot, который используется Debezium.

---

# 5. Таблицы PostgreSQL

Создаются две таблицы.

## users

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## orders

```sql
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product_name VARCHAR(100),
    quantity INT,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Между таблицами существует связь:

```text
users.id
   ▲
   │ Foreign Key
   │
orders.user_id
```

---

# 6. Первоначальные тестовые данные

В таблицу `users` добавляются:

```sql
INSERT INTO users (name, email)
VALUES ('John Doe', 'john@example.com');

INSERT INTO users (name, email)
VALUES ('Jane Smith', 'jane@example.com');

INSERT INTO users (name, email)
VALUES ('Alice Johnson', 'alice@example.com');

INSERT INTO users (name, email)
VALUES ('Bob Brown', 'bob@example.com');
```

В `orders`:

```sql
INSERT INTO orders (user_id, product_name, quantity)
VALUES (1, 'Product A', 2);

INSERT INTO orders (user_id, product_name, quantity)
VALUES (1, 'Product B', 1);

INSERT INTO orders (user_id, product_name, quantity)
VALUES (2, 'Product C', 5);

INSERT INTO orders (user_id, product_name, quantity)
VALUES (3, 'Product D', 3);

INSERT INTO orders (user_id, product_name, quantity)
VALUES (4, 'Product E', 4);
```

---

# 7. Запуск проекта

Перейти в директорию:

```bash
cd 4_module
```

Перед первой сборкой необходимо скачать JMX Prometheus Java Agent:

```bash
curl -L \
  -o kafka-connect/jmx_prometheus_javaagent-0.15.0.jar \
  https://repo.maven.apache.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.15.0/jmx_prometheus_javaagent-0.15.0.jar
```

Проверить Docker Compose:

```bash
docker compose config
```

Запустить инфраструктуру:

```bash
docker compose up -d --build
```

Проверить контейнеры:

```bash
docker compose ps
```

Ожидаемые сервисы:

```text
kafka-0
kafka-1
kafka-2
postgres
kafka-connect
kafka-ui
prometheus
grafana
```

---

# 8. Проверка PostgreSQL

Подключиться:

```bash
docker compose exec postgres \
  psql -U postgres-user -d customers
```

Проверить таблицы:

```sql
\dt
```

Проверить данные:

```sql
SELECT * FROM users;

SELECT * FROM orders;
```

Проверить logical replication:

```sql
SHOW wal_level;

SHOW max_wal_senders;

SHOW max_replication_slots;
```

Ожидаемые значения:

```text
wal_level             = logical
max_wal_senders       = 1
max_replication_slots = 1
```

Выход:

```text
\q
```

---

# 9. Проверка Kafka Connect

REST API:

```bash
curl -s http://localhost:38083
```

Проверить установленные plugins:

```bash
curl -s http://localhost:38083/connector-plugins \
  | python3 -m json.tool
```

Среди plugins должен присутствовать:

```text
io.debezium.connector.postgresql.PostgresConnector
```

Проверить список созданных connectors:

```bash
curl -s http://localhost:38083/connectors
```

---

# 10. Конфигурация Debezium

Файл:

```text
connector/debezium-postgres.json
```

Основная конфигурация:

```json
{
  "name": "pg-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",

    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres-user",
    "database.password": "postgres-pw",
    "database.dbname": "customers",

    "topic.prefix": "customers",

    "plugin.name": "pgoutput",
    "slot.name": "debezium_practice_5",
    "publication.name": "dbz_publication_practice_5",
    "publication.autocreate.mode": "filtered",

    "schema.include.list": "public",
    "table.include.list": "public.users,public.orders",

    "snapshot.mode": "initial"
  }
}
```

Connector отслеживает только:

```text
public.users
public.orders
```

В результате создаются Kafka topics:

```text
customers.public.users
customers.public.orders
```

---

# 11. Создание Debezium Connector

Проверить JSON:

```bash
python3 -m json.tool connector/debezium-postgres.json
```

Создать Connector:

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  --data @connector/debezium-postgres.json \
  http://localhost:38083/connectors
```

Проверить статус:

```bash
curl -s \
  http://localhost:38083/connectors/pg-connector/status \
  | python3 -m json.tool
```

Ожидается:

```text
Connector → RUNNING
Task 0    → RUNNING
```

Проверить список connectors:

```bash
curl -s http://localhost:38083/connectors
```

Результат:

```json
["pg-connector"]
```

---

# 12. Проверка Kafka topics

```bash
docker compose exec kafka-0 \
  kafka-topics.sh \
  --bootstrap-server kafka-0:9092 \
  --list
```

После запуска Debezium должны существовать:

```text
customers.public.users
customers.public.orders
```

А также служебные topics Kafka Connect:

```text
connect-configs
connect-offsets
connect-statuses
```

Kafka Connect использует их для хранения:

```text
connect-configs
    → конфигураций connectors

connect-offsets
    → offsets connectors

connect-statuses
    → состояния connectors/tasks
```

---

# 13. Проверка Replication Slot

```bash
docker compose exec postgres \
  psql -U postgres-user -d customers \
  -c "SELECT slot_name, plugin, slot_type, active FROM pg_replication_slots;"
```

В ходе работы был создан:

```text
slot_name  = debezium_practice_5
plugin     = pgoutput
slot_type  = logical
active     = true
```

---

# 14. Проверка Publication

```bash
docker compose exec postgres \
  psql -U postgres-user -d customers \
  -c "SELECT pubname, puballtables FROM pg_publication;"
```

Publication:

```text
dbz_publication_practice_5
```

Проверка отслеживаемых таблиц:

```bash
docker compose exec postgres \
  psql -U postgres-user -d customers \
  -c "SELECT pubname, schemaname, tablename FROM pg_publication_tables WHERE pubname = 'dbz_publication_practice_5';"
```

Результат:

```text
public.users
public.orders
```

Таким образом Debezium отслеживает только необходимые таблицы.

---

# 15. Initial Snapshot

Используется:

```json
"snapshot.mode": "initial"
```

При первом запуске Debezium сначала считывает уже существующие строки.

Схема:

```text
Debezium starts
      │
      ▼
Initial Snapshot
      │
      ├── users
      └── orders
      │
      ▼
Kafka
      │
      ▼
Streaming from WAL
```

Snapshot-события имеют:

```text
op = r
```

где:

```text
r = READ
```

Для первоначальных данных было получено:

```text
users  → 4 snapshot records
orders → 5 snapshot records
```

---

# 16. Чтение Kafka events через CLI

## users

```bash
docker compose exec kafka-0 \
  kafka-console-consumer.sh \
  --bootstrap-server kafka-0:9092 \
  --topic customers.public.users \
  --from-beginning \
  --property print.key=true \
  --property print.value=true
```

## orders

```bash
docker compose exec kafka-0 \
  kafka-console-consumer.sh \
  --bootstrap-server kafka-0:9092 \
  --topic customers.public.orders \
  --from-beginning \
  --property print.key=true \
  --property print.value=true
```

---

# 17. Проверка Live CDC

Debezium обрабатывает операции:

```text
r → READ / snapshot
c → CREATE / INSERT
u → UPDATE
d → DELETE
```

## INSERT

```sql
INSERT INTO users (name, email)
VALUES ('Test User', 'test@example.com');
```

В Kafka:

```text
Operation: c
```

---

## UPDATE

```sql
UPDATE users
SET email = 'updated@example.com'
WHERE id = 5;
```

В Kafka:

```text
Operation: u
```

---

## DELETE

```sql
DELETE FROM users
WHERE id = 5;
```

В Kafka:

```text
Operation: d
```

После DELETE Debezium также может отправить Kafka tombstone:

```text
key   = удалённый record key
value = null
```

Python consumer учитывает такие сообщения и не пытается выполнять JSON decode для `None`.

---

# 18. Python Consumer

Python consumer находится:

```text
app/consumer.py
```

Он подписывается сразу на два topics:

```python
TOPICS = [
    "customers.public.users",
    "customers.public.orders",
]
```

Kafka bootstrap servers:

```python
KAFKA_BROKERS = "localhost:39094,localhost:39095,localhost:39096"
```

Consumer выводит:

```text
Topic
Partition
Offset
Key
Operation
Before
After
```

---

## Установка Python dependencies

Создать или активировать virtual environment.

Пример:

```bash
source ../.venv/bin/activate
```

Установить зависимости:

```bash
pip install -r requirements.txt
```

---

## Запуск Consumer

Из директории `4_module`:

```bash
python app/consumer.py
```

Пример полученного live CDC event:

```text
Topic:     customers.public.users
Partition: 0
Operation: c

Before:
null

After:
{
  "id": 6,
  "name": "Python Consumer Test",
  "email": "python@example.com"
}
```

---

# 19. Tombstone handling

При DELETE Kafka record может иметь:

```python
msg.value() is None
```

Поэтому consumer выполняет проверку:

```python
if msg.value() is None:
    print("Tombstone: value = null")
    continue
```

Это предотвращает ошибку:

```text
AttributeError:
'NoneType' object has no attribute 'decode'
```

---

# 20. JMX Exporter

Для мониторинга Kafka Connect используется Prometheus JMX Exporter.

Кастомный Kafka Connect image создаётся через:

```text
kafka-connect/Dockerfile
```

```dockerfile
FROM quay.io/debezium/connect:3.2

COPY jmx_prometheus_javaagent-0.15.0.jar /opt/jmx_prometheus_javaagent-0.15.0.jar

COPY jmx-config.yml /opt/jmx-config.yml
```

В Kafka Connect JVM передаётся:

```text
-javaagent:/opt/jmx_prometheus_javaagent-0.15.0.jar=9876:/opt/jmx-config.yml
```

Endpoint:

```text
http://localhost:39876/metrics
```

Проверка:

```bash
curl -s http://localhost:39876/metrics | head -40
```

---

# 21. Kafka Connect Metrics

Проверить Kafka Connect metrics:

```bash
curl -s http://localhost:39876/metrics \
  | grep kafka_connect \
  | head -30
```

В частности доступны:

```text
kafka_connect_source_task_metrics_source_record_poll_total

kafka_connect_source_task_metrics_source_record_poll_rate

kafka_connect_source_task_metrics_source_record_write_total

kafka_connect_source_task_metrics_source_record_write_rate

kafka_connect_source_task_metrics_source_record_active_count
```

---

# 22. Prometheus

Конфигурация:

```text
prometheus/prometheus.yml
```

```yaml
global:
  scrape_interval: 5s
  evaluation_interval: 5s

scrape_configs:

  - job_name: "prometheus"
    static_configs:
      - targets:
          - "localhost:9090"

  - job_name: "kafka-connect"
    metrics_path: /metrics

    static_configs:
      - targets:
          - "kafka-connect:9876"
```

Prometheus подключается к Kafka Connect внутри Docker network:

```text
Prometheus
     │
     ▼
kafka-connect:9876
```

---

# 23. Проверка Prometheus

Открыть:

```text
http://localhost:39090
```

Проверка Kafka Connect target:

```promql
up{job="kafka-connect"}
```

Нормальное значение:

```text
1
```

Это означает:

```text
Prometheus → JMX Exporter → UP
```

---

# 24. Генерация тестовой нагрузки

Для появления данных на графиках можно создать несколько CDC events:

```bash
docker compose exec postgres \
  psql -U postgres-user -d customers
```

Например:

```sql
INSERT INTO users (name, email)
SELECT
    'Metrics User ' || n,
    'metrics' || n || '@example.com'
FROM generate_series(1, 20) AS n;
```

Путь данных:

```text
20 INSERT
    │
    ▼
PostgreSQL WAL
    │
    ▼
Debezium
    │
    ▼
Kafka Connect
    │
    ▼
Kafka
```

После этого показатели Kafka Connect меняются и становятся видимыми в Prometheus/Grafana.

---

# 25. Grafana

Grafana:

```text
http://localhost:33000
```

Для подключения Prometheus используется internal Docker URL:

```text
http://prometheus:9090
```

Необходимо создать Prometheus Data Source и выполнить:

```text
Save & Test
```

---

# 26. Grafana Dashboard

Для мониторинга Kafka Connect были созданы следующие панели.

## Kafka Connect Status

PromQL:

```promql
up{job="kafka-connect"}
```

Visualization:

```text
Stat
```

Значение:

```text
1 = Kafka Connect metrics доступны
```

---

## Assigned Connectors

```promql
kafka_connect_connect_coordinator_metrics_assigned_connectors
```

Показывает количество connectors, назначенных Worker.

При одном `pg-connector`:

```text
1
```

---

## Source Record Poll Rate

```promql
rate(
  kafka_connect_source_task_metrics_source_record_poll_total{
    connector="pg-connector",
    task="0"
  }[1m]
)
```

Показывает количество records в секунду, получаемых Source Task от источника.

---

## Source Record Write Rate

```promql
rate(
  kafka_connect_source_task_metrics_source_record_write_total{
    connector="pg-connector",
    task="0"
  }[1m]
)
```

Показывает скорость успешной записи CDC records в Kafka.

---

## Active Records

```promql
kafka_connect_source_task_metrics_source_record_active_count{
  connector="pg-connector",
  task="0"
}
```

Показывает количество records, которые уже созданы Source Task, но ещё не полностью записаны в Kafka.

---

# 27. Итоговая схема мониторинга

```text
Kafka Connect JVM
       │
       │ JMX
       ▼
Prometheus JMX Exporter
       │
       │ HTTP /metrics
       ▼
Prometheus
       │
       │ PromQL
       ▼
Grafana
       │
       ▼
Dashboard
```

---

# 28. Полезные URL

| Сервис | URL |
|---|---|
| Kafka UI | `http://localhost:38080` |
| Kafka Connect REST API | `http://localhost:38083` |
| Kafka Connect JMX Metrics | `http://localhost:39876/metrics` |
| Prometheus | `http://localhost:39090` |
| Grafana | `http://localhost:33000` |

---

# 29. Остановка проекта

Остановить containers:

```bash
docker compose down
```

Named volumes при этом сохраняются.

То есть данные Kafka, PostgreSQL и Grafana не удаляются.

Повторный запуск:

```bash
docker compose up -d
```

---

## Полное удаление данных

Команда:

```bash
docker compose down -v
```

удаляет containers **и named volumes**.

После этого будут потеряны локальные данные:

```text
Kafka
PostgreSQL
Grafana
```

После следующего запуска PostgreSQL снова выполнит `init.sql`, так как будет создан новый пустой PostgreSQL volume.

---

# 30. Итог

В результате практической работы построена полноценная CDC-инфраструктура:

```text
PostgreSQL
     │
     ▼
Logical WAL
     │
     ▼
Debezium PostgreSQL Connector
     │
     ▼
Kafka Connect
     │
     ▼
Apache Kafka
     │
     ├── customers.public.users
     └── customers.public.orders
     │
     ▼
Python Consumer
```

Также реализован мониторинг:

```text
Kafka Connect
     │
     ▼
JMX Exporter
     │
     ▼
Prometheus
     │
     ▼
Grafana
```

Проверены:

```text
PostgreSQL logical replication      
Replication Slot                    
Publication                         
Table filtering                     
Debezium Connector RUNNING          
Debezium Task 0 RUNNING             
Initial Snapshot                    
INSERT CDC                          
UPDATE CDC                          
DELETE CDC                          
Kafka Tombstone handling            
Python Consumer                     
JMX Exporter                        
Prometheus scraping                 
Grafana dashboard                   
```

Практическая работа завершена.