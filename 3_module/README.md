# Практическая работа 4 — Балансировка партиций и диагностика Apache Kafka

## Цель работы

Цель практической работы — изучить инструменты администрирования Apache Kafka для:

- создания топика с несколькими партициями и репликами;
- просмотра текущего распределения партиций;
- ручного переназначения реплик с помощью `kafka-reassign-partitions.sh`;
- проверки выполнения reassignment;
- моделирования отказа Kafka broker;
- анализа `Leader`, `Replicas` и `ISR`;
- проверки восстановления реплик после возврата broker в кластер.

---

## Архитектура

В практической работе используется Kafka-кластер из трёх broker:

```text
                Kafka Cluster

        ┌─────────────────────┐
        │      kafka-0        │
        │ Broker ID: 0        │
        └─────────────────────┘

        ┌─────────────────────┐
        │      kafka-1        │
        │ Broker ID: 1        │
        └─────────────────────┘

        ┌─────────────────────┐
        │      kafka-2        │
        │ Broker ID: 2        │
        └─────────────────────┘

                  │
                  ▼

             Kafka UI
```

Kafka работает в режиме **KRaft**, без ZooKeeper.

Для управления кластером используется `docker compose`.

---

## Структура проекта

```text
3_module/
├── docker-compose.yml
├── reassignment.json
└── README.md
```

Назначение файлов:

- `docker-compose.yml` — Kafka-кластер из трёх broker и Kafka UI;
- `reassignment.json` — новое распределение replicas для партиций;
- `README.md` — описание выполнения практической работы.

---

# 1. Запуск Kafka-кластера

Запуск контейнеров:

```bash
docker compose up -d
```

Проверка состояния:

```bash
docker compose ps
```

В кластере запускаются:

```text
kafka-0
kafka-1
kafka-2
kafka-ui
```

Kafka UI доступен по адресу:

```text
http://localhost:28080
```

---

# 2. Создание топика

Для выполнения административных команд подключаемся к контейнеру `kafka-0`:

```bash
docker compose exec kafka-0 bash
```

Создаём топик `balanced_topic`:

```bash
kafka-topics.sh \
  --bootstrap-server kafka-0:9092 \
  --create \
  --topic balanced_topic \
  --partitions 8 \
  --replication-factor 3
```

Результат:

```text
Created topic balanced_topic.
```

Топик имеет:

```text
Partitions:          8
Replication Factor:  3
Brokers:             3
```

Так как количество broker равно `3`, а `Replication Factor = 3`, каждая partition имеет replica на каждом broker.

---

# 3. Просмотр первоначального распределения

Для просмотра распределения партиций:

```bash
kafka-topics.sh \
  --bootstrap-server kafka-0:9092 \
  --describe \
  --topic balanced_topic
```

Первоначальное распределение:

```text
Partition 0  Leader: 2  Replicas: 2,0,1  ISR: 2,0,1
Partition 1  Leader: 0  Replicas: 0,1,2  ISR: 0,1,2
Partition 2  Leader: 1  Replicas: 1,2,0  ISR: 1,2,0
Partition 3  Leader: 1  Replicas: 1,2,0  ISR: 1,2,0
Partition 4  Leader: 2  Replicas: 2,0,1  ISR: 2,0,1
Partition 5  Leader: 0  Replicas: 0,1,2  ISR: 0,1,2
Partition 6  Leader: 2  Replicas: 2,1,0  ISR: 2,1,0
Partition 7  Leader: 1  Replicas: 1,0,2  ISR: 1,0,2
```

Где:

- `Leader` — broker, который в данный момент является лидером partition;
- `Replicas` — broker, на которых должны храниться replicas partition;
- `ISR` (`In-Sync Replicas`) — replicas, которые сейчас синхронизированы с Leader.

---

# 4. Создание нового распределения

Для ручного переназначения replicas был создан файл:

```text
reassignment.json
```

Содержимое:

```json
{
  "version": 1,
  "partitions": [
    {
      "topic": "balanced_topic",
      "partition": 0,
      "replicas": [0, 1, 2],
      "log_dirs": ["any", "any", "any"]
    },
    {
      "topic": "balanced_topic",
      "partition": 1,
      "replicas": [1, 2, 0],
      "log_dirs": ["any", "any", "any"]
    },
    {
      "topic": "balanced_topic",
      "partition": 2,
      "replicas": [2, 0, 1],
      "log_dirs": ["any", "any", "any"]
    },
    {
      "topic": "balanced_topic",
      "partition": 3,
      "replicas": [0, 2, 1],
      "log_dirs": ["any", "any", "any"]
    },
    {
      "topic": "balanced_topic",
      "partition": 4,
      "replicas": [1, 0, 2],
      "log_dirs": ["any", "any", "any"]
    },
    {
      "topic": "balanced_topic",
      "partition": 5,
      "replicas": [2, 1, 0],
      "log_dirs": ["any", "any", "any"]
    },
    {
      "topic": "balanced_topic",
      "partition": 6,
      "replicas": [0, 1, 2],
      "log_dirs": ["any", "any", "any"]
    },
    {
      "topic": "balanced_topic",
      "partition": 7,
      "replicas": [1, 2, 0],
      "log_dirs": ["any", "any", "any"]
    }
  ]
}
```

План изменения:

```text
Partition     Было        Стало

P0            2,0,1   ->  0,1,2
P1            0,1,2   ->  1,2,0
P2            1,2,0   ->  2,0,1
P3            1,2,0   ->  0,2,1
P4            2,0,1   ->  1,0,2
P5            0,1,2   ->  2,1,0
P6            2,1,0   ->  0,1,2
P7            1,0,2   ->  1,2,0
```

В данной конфигурации используются три broker и `Replication Factor = 3`.

Поэтому каждая partition уже хранится на всех трёх broker.

Reassignment в данном случае изменяет порядок replicas и preferred replica, а не переносит partition на четвёртый broker.

---

# 5. Передача reassignment-файла в контейнер

Файл копируется в контейнер `kafka-0`:

```bash
docker compose cp reassignment.json kafka-0:/tmp/reassignment.json
```

Проверка:

```bash
docker compose exec kafka-0 cat /tmp/reassignment.json
```

---

# 6. Выполнение Partition Reassignment

Подключаемся к `kafka-0`:

```bash
docker compose exec kafka-0 bash
```

Запускаем reassignment:

```bash
kafka-reassign-partitions.sh \
  --bootstrap-server kafka-0:9092 \
  --reassignment-json-file /tmp/reassignment.json \
  --execute
```

Kafka сохранила информацию о предыдущем распределении и начала переназначение:

```text
Successfully started partition reassignments for
balanced_topic-0,
balanced_topic-1,
balanced_topic-2,
balanced_topic-3,
balanced_topic-4,
balanced_topic-5,
balanced_topic-6,
balanced_topic-7
```

При выполнении `--execute` Kafka также выводит предыдущий assignment:

```text
Save this to use as the --reassignment-json-file option during rollback
```

Его можно сохранить и использовать для возврата к предыдущей конфигурации.

---

# 7. Проверка выполнения reassignment

Для проверки используется параметр `--verify`:

```bash
kafka-reassign-partitions.sh \
  --bootstrap-server kafka-0:9092 \
  --reassignment-json-file /tmp/reassignment.json \
  --verify
```

Полученный результат:

```text
Status of partition reassignment:

Reassignment of partition balanced_topic-0 is completed.
Reassignment of partition balanced_topic-1 is completed.
Reassignment of partition balanced_topic-2 is completed.
Reassignment of partition balanced_topic-3 is completed.
Reassignment of partition balanced_topic-4 is completed.
Reassignment of partition balanced_topic-5 is completed.
Reassignment of partition balanced_topic-6 is completed.
Reassignment of partition balanced_topic-7 is completed.
```

Все восемь partition были успешно переназначены.

---

# 8. Проверка нового распределения

После reassignment:

```bash
kafka-topics.sh \
  --bootstrap-server kafka-0:9092 \
  --describe \
  --topic balanced_topic
```

Получено новое распределение:

```text
Partition 0  Leader: 2  Replicas: 0,1,2  ISR: 2,0,1
Partition 1  Leader: 0  Replicas: 1,2,0  ISR: 0,1,2
Partition 2  Leader: 1  Replicas: 2,0,1  ISR: 1,2,0
Partition 3  Leader: 1  Replicas: 0,2,1  ISR: 1,2,0
Partition 4  Leader: 2  Replicas: 1,0,2  ISR: 2,0,1
Partition 5  Leader: 0  Replicas: 2,1,0  ISR: 0,1,2
Partition 6  Leader: 2  Replicas: 0,1,2  ISR: 2,1,0
Partition 7  Leader: 1  Replicas: 1,2,0  ISR: 1,0,2
```

Таким образом значение `Replicas` изменилось согласно `reassignment.json`.

Важно:

```text
Leader
```

не обязан сразу совпадать с первой replica после reassignment.

Например:

```text
Partition 0

Leader:   2
Replicas: 0,1,2
```

Первая replica в assignment является preferred replica, однако текущий Leader может некоторое время оставаться другим broker.

---

# 9. Моделирование сбоя broker

Для моделирования отказа был остановлен:

```text
kafka-1
```

Команда:

```bash
docker compose stop kafka-1
```

После остановки выполняется:

```bash
docker compose exec kafka-0 \
  kafka-topics.sh \
  --bootstrap-server kafka-0:9092 \
  --describe \
  --topic balanced_topic
```

Получено состояние:

```text
Partition 0  Leader: 0  Replicas: 0,1,2  ISR: 2,0
Partition 1  Leader: 2  Replicas: 1,2,0  ISR: 0,2
Partition 2  Leader: 2  Replicas: 2,0,1  ISR: 2,0
Partition 3  Leader: 0  Replicas: 0,2,1  ISR: 2,0
Partition 4  Leader: 0  Replicas: 1,0,2  ISR: 2,0
Partition 5  Leader: 2  Replicas: 2,1,0  ISR: 0,2
Partition 6  Leader: 0  Replicas: 0,1,2  ISR: 2,0
Partition 7  Leader: 2  Replicas: 1,2,0  ISR: 0,2
```

---

## Что произошло после сбоя

Broker `1` остался в списке:

```text
Replicas
```

но исчез из:

```text
ISR
```

Например:

```text
Replicas: 0,1,2
ISR:      2,0
```

Это означает:

```text
Replicas
=
broker, на которых по конфигурации должны находиться копии partition

ISR
=
broker, которые сейчас работают и имеют синхронизированные replicas
```

Kafka не удаляет `kafka-1` из assignment только потому, что broker временно недоступен.

---

## Leader Election

После остановки `kafka-1` Kafka автоматически выбрала новые Leader для partition, которым требовалась смена лидера.

Например:

```text
Partition 2

до сбоя:
Leader: 1

после сбоя:
Leader: 2
```

Kafka выбрала нового Leader из оставшихся синхронизированных replicas.

Таким образом кластер продолжил работу после отказа одного broker.

---

# 10. Восстановление broker

Broker был снова запущен:

```bash
docker compose start kafka-1
```

После запуска повторно проверяется состояние:

```bash
docker compose exec kafka-0 \
  kafka-topics.sh \
  --bootstrap-server kafka-0:9092 \
  --describe \
  --topic balanced_topic
```

Получено:

```text
Partition 0  Leader: 0  Replicas: 0,1,2  ISR: 2,0,1
Partition 1  Leader: 2  Replicas: 1,2,0  ISR: 0,2,1
Partition 2  Leader: 2  Replicas: 2,0,1  ISR: 2,0,1
Partition 3  Leader: 0  Replicas: 0,2,1  ISR: 2,0,1
Partition 4  Leader: 0  Replicas: 1,0,2  ISR: 2,0,1
Partition 5  Leader: 2  Replicas: 2,1,0  ISR: 0,2,1
Partition 6  Leader: 0  Replicas: 0,1,2  ISR: 2,0,1
Partition 7  Leader: 2  Replicas: 1,2,0  ISR: 0,2,1
```

Broker `1` снова появился в ISR всех partition.

Это означает, что после запуска broker:

```text
kafka-1 запускается
        ↓
восстанавливает соединение с кластером
        ↓
догоняет актуальное состояние partition
        ↓
становится синхронизированной replica
        ↓
возвращается в ISR
```

Синхронизация replicas успешно восстановилась.

---

# Основные выводы

В ходе практической работы были изучены механизмы балансировки и отказоустойчивости Apache Kafka.

### Partition Reassignment

Команда:

```text
kafka-reassign-partitions.sh
```

позволяет изменять назначение replicas конкретных partition между Kafka broker.

Последовательность ручного reassignment:

```text
reassignment.json
        ↓
--execute
        ↓
Kafka применяет новое распределение
        ↓
--verify
        ↓
проверка завершения операции
```

### Replicas и ISR

```text
Replicas
```

показывает, на каких broker по конфигурации должны храниться replicas partition.

```text
ISR
```

показывает, какие из назначенных replicas сейчас синхронизированы с Leader.

При отключении `kafka-1`:

```text
Replicas: 0,1,2
ISR:      0,2
```

После восстановления:

```text
Replicas: 0,1,2
ISR:      0,2,1
```

### Отказоустойчивость

Благодаря `Replication Factor = 3` кластер продолжил работу после отключения одного broker.

Если недоступный broker являлся Leader partition, Kafka могла выбрать нового Leader из оставшихся ISR.

После запуска broker обратно его replicas синхронизировались и снова вошли в ISR.

---

# Остановка кластера

Остановить контейнеры:

```bash
docker compose down
```

Остановить контейнеры с удалением volumes:

```bash
docker compose down -v
```

> `-v` удаляет Kafka volumes и сохранённые данные кластера.