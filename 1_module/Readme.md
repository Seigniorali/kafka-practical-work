# Практическая работа 2 — Apache Kafka

## Тема

Настройка Kafka-кластера и реализация Producer с двумя Consumer в разных group

Проект реализован на Python с использованием библиотеки `confluent-kafka`. Для запуска используется локальный Kafka-кластер из трёх узлов в режиме KRaft без ZooKeeper

---

## Что проверяется в работе

Для подтверждения выполнения задания необходимо проверить:

- topic `my-topic` имеет 3 partitions и replication factor 2
- Producer отправляет сообщения в Kafka
- `SingleMessageConsumer` читает сообщения по одному и использует auto commit
- `BatchMessageConsumer` обрабатывает сообщения пачками минимум по 10 и выполняет один synchronous commit после пачки
- оба типа Consumer используют разные `group.id` и независимо читают сообщения
- приложение запускается в двух экземплярах через Docker Compose
- в каждой Consumer Group отображаются 2 members
- Kafka распределяет partitions между двумя экземплярами Consumer

---

## Архитектура проекта

Kafka-кластер состоит из трёх узлов:

- `kafka-0`
- `kafka-1`
- `kafka-2`

Каждый узел одновременно выполняет роли `broker` и `controller`

Для просмотра состояния Kafka используется Kafka UI

Приложение состоит из:

- `Producer`
- `SingleMessageConsumer`
- `BatchMessageConsumer`

Приложение запускается в двух экземплярах через Docker Compose

                       Kafka Cluster

             kafka-0   kafka-1   kafka-2
                 \        |        /
                  \       |       /
                       my-topic
        Partition 0   Partition 1   Partition 2
                         |
              +----------+----------+
              |                     |
              v                     v
          kafka-app-1           kafka-app-2
          -----------           -----------
          Producer              Producer
          SingleConsumer        SingleConsumer
          BatchConsumer         BatchConsumer


Оба экземпляра приложения используют одинаковые `group.id` для Consumer одного типа, поэтому Kafka распределяет partitions между двумя экземплярами внутри каждой Consumer Group

Например:

single-message-group
Consumer #1 -> Partition 2
Consumer #2 -> Partition 0, 1

При этом `SingleMessageConsumer` и `BatchMessageConsumer` находятся в разных Consumer Group, поэтому оба типа Consumer могут независимо читать одни и те же сообщения

---

## Структура проекта

Kafka/
├── .venv/
└── 1 module/
    ├── app/
    │   ├── main.py
    │   ├── message.py
    │   ├── producer.py
    │   ├── single_consumer.py
    │   └── batch_consumer.py
    ├── Dockerfile
    ├── docker-compose.yml
    ├── requirements.txt
    ├── topic.txt
    └── Readme.md

---

## Kafka Topic

Для работы приложения используется topic `my-topic`

Параметры topic:

Partitions:          3
Replication Factor:  2
min.insync.replicas: 2


Topic создаётся вручную через Kafka CLI в соответствии с условием задания

Команда создания topic:

docker exec -it kafka-0 \
  kafka-topics.sh \
  --create \
  --topic my-topic \
  --partitions 3 \
  --replication-factor 2 \
  --bootstrap-server kafka-0:9092

Проверка topic:

docker exec -it kafka-0 \
  kafka-topics.sh \
  --describe \
  --topic my-topic \
  --bootstrap-server kafka-0:9092

Полный пример команды создания и вывода `--describe` находится в файле `topic.txt`

---

## Message

Модель сообщения определена в `app/message.py`

Используется Python `dataclass`:

@dataclass
class Message:
    id: int
    text: str

Пример сообщения:

{
  "id": 1,
  "text": "Message number 1"
}

Перед отправкой объект `Message` сериализуется в JSON

Consumer получает `bytes`, декодирует их в UTF-8, преобразует JSON в Python dictionary и создаёт объект `Message`

В случае ошибки десериализации сообщение об ошибке выводится в консоль

---

## Producer

Producer реализован в `app/producer.py`

Он создаёт объекты `Message`, сериализует их в JSON и отправляет в `my-topic`

Основные параметры Producer

"bootstrap.servers": KAFKA_SERVERS
"acks": "all"
"retries": 5

### `acks=all`

Producer ожидает подтверждение Kafka после успешной записи сообщения в необходимое количество ISR-реплик

Вместе с настройкой Kafka, обеспечивает более надёжную запись сообщений:

min.insync.replicas=2

### `retries=5`

Если при отправке возникает временная ошибка, Producer повторяет попытку отправки сообщения. В рамках задания комбинация `acks=all` и `retries=5` используется для реализации требуемой гарантии доставки At Least Once

### Асинхронная отправка

Для отправки используется:

producer.produce(...)

Метод работает асинхронно

Callback `delivery_report()` выводит результат доставки: `topic`, `partition` и `offset`

Метод:

producer.poll(0) - обрабатывает внутренние события Producer и callback доставки

Перед завершением вызывается:

producer.flush() - чтобы дождаться отправки оставшихся сообщений

---

## SingleMessageConsumer

Consumer реализован в `app/single_consumer.py`

Он использует Consumer Group:

single-message-group

Основные параметры:

"group.id": "single-message-group"
"auto.offset.reset": "earliest"
"enable.auto.commit": True
"auto.commit.interval.ms": 1000

Сообщения считываются по одному:

consumer.poll(1.0)

После получения сообщение десериализуется, после чего в консоль выводятся `id`, `text`, `key`, `partition` и `offset`

Commit offset выполняется автоматически

Если возникает ошибка Kafka или ошибка обработки сообщения, она выводится в консоль, после чего Consumer продолжает работу

---

## BatchMessageConsumer

Batch Consumer реализован в `app/batch_consumer.py`.

Он использует отдельную Consumer Group:

batch-message-group


Благодаря разным `group.id`, `SingleMessageConsumer` и `BatchMessageConsumer` независимо читают одни и те же сообщения

Автоматический commit отключён:

"enable.auto.commit": False

Размер batch:

BATCH_SIZE = 10

Для получения сообщений используется:


consumer.consume(...)


В `confluent-kafka-python` метод `poll()` возвращает одно сообщение. Для batch-чтения используется `consume(num_messages=...)`, который может вернуть меньше запрошенного количества сообщений.

Поэтому Consumer дополнительно накапливает сообщения в список:


while len(batch) < BATCH_SIZE:


Обработка начинается только после накопления минимум 10 сообщений

После обработки всей пачки выполняется один синхронный commit:


consumer.commit(asynchronous=False)


Принцип работы:

10 сообщений
      |
      v
обработка каждого сообщения
      |
      v
один synchronous commit

Пример результата:

kafka-app-1  | Получена пачка: 10 сообщений
kafka-app-1  | Обрабатываем: id=1, text='Message number 1', key=1, partition=2, offset=0
kafka-app-1  | Обрабатываем: id=1, text='Message number 1', key=1, partition=2, offset=1
kafka-app-1  | Обрабатываем: id=8, text='Message number 8', key=8, partition=2, offset=2
kafka-app-1  | Обрабатываем: id=8, text='Message number 8', key=8, partition=2, offset=3
kafka-app-1  | Обрабатываем: id=13, text='Message number 13', key=13, partition=2, offset=4
kafka-app-1  | Обрабатываем: id=13, text='Message number 13', key=13, partition=2, offset=5
kafka-app-1  | Обрабатываем: id=17, text='Message number 17', key=17, partition=2, offset=6
kafka-app-1  | Обрабатываем: id=17, text='Message number 17', key=17, partition=2, offset=7
kafka-app-1  | Обрабатываем: id=22, text='Message number 22', key=22, partition=2, offset=8
kafka-app-1  | Обрабатываем: id=22, text='Message number 22', key=22, partition=2, offset=9
kafka-app-1  | Batch из 10 сообщений обработан. Offset успешно закоммичен.

---

## main.py

Файл `app/main.py` объединяет Producer и оба Consumer в одно приложение.

`SingleMessageConsumer` и `BatchMessageConsumer` запускаются в отдельных Python threads:

main.py
│
├── SingleConsumer thread
├── BatchConsumer thread
└── Producer


Сначала запускаются оба Consumer. После этого приложение ждёт несколько секунд:

time.sleep(5)


Это позволяет Consumer подключиться к Kafka и получить назначение partitions. После этого запускается Producer

После завершения Producer основной процесс остаётся запущенным, чтобы Consumer продолжали принимать новые сообщения

---

## Переменные окружения

Для возможности запуска приложения как локально, так и внутри Docker используются environment variables.

Producer использует:

KAFKA_SERVERS
TOPIC
MESSAGE_COUNT


Consumer используют:

KAFKA_SERVERS
TOPIC


При запуске Python-приложения локально на host-машине используются внешние адреса Kafka:

localhost:9094
localhost:9095
localhost:9096


При запуске приложения внутри Docker используются внутренние адреса брокеров:

kafka-0:9092
kafka-1:9092
kafka-2:9092

В `docker-compose.yml`:

environment:
  KAFKA_SERVERS: "kafka-0:9092,kafka-1:9092,kafka-2:9092"
  TOPIC: "my-topic"
  MESSAGE_COUNT: "60"

---

## Запуск двух экземпляров приложения

В Docker Compose используется:

deploy:
  replicas: 2


В результате создаются два контейнера приложения:

kafka-kafka-app-1
kafka-kafka-app-2

docker ps
CONTAINER ID   IMAGE                           COMMAND                  CREATED              STATUS              PORTS                                         NAMES
d4b08fb642af   kafka-kafka-app                 "python main.py"         13 seconds ago       Up 13 seconds                                                     kafka-kafka-app-1
b78187c2732c   kafka-kafka-app                 "python main.py"         13 seconds ago       Up 13 seconds                                                     kafka-kafka-app-2

Каждый экземпляр содержит:

1 Producer
1 SingleMessageConsumer
1 BatchMessageConsumer


Таким образом условие запуска приложения в двух экземплярах выполняется

Так как Producer существует в каждом экземпляре приложения, каждый Producer отправляет собственный набор сообщений

При `MESSAGE_COUNT=60`:

Producer #1 -> 60 сообщений
Producer #2 -> 60 сообщений

Всего -> 120 сообщений


Поэтому сообщения могут иметь одинаковые `id`, но разные Kafka offset. Это два разных сообщения, созданных двумя разными экземплярами Producer

---

## Consumer Groups и распределение partitions

В каждой Consumer Group работают два экземпляра Consumer

Проверить `SingleMessageConsumer`:

docker exec -it kafka-0 \
  kafka-consumer-groups.sh \
  --bootstrap-server kafka-0:9092 \
  --group single-message-group \
  --describe --members --verbose


Пример результата:

docker exec -it kafka-0 \       
  kafka-consumer-groups.sh \
  --bootstrap-server kafka-0:9092 \
  --group single-message-group \
  --describe --members --verbose

GROUP                CONSUMER-ID                                  HOST            CLIENT-ID       #PARTITIONS     ASSIGNMENT
single-message-group rdkafka-f033af28-d89c-4a25-b978-19fe4d526dee /172.18.0.6     rdkafka         1               my-topic(2)
single-message-group rdkafka-d80c0d07-2c0c-4f3d-a4a6-6d73306f1e70 /172.18.0.7     rdkafka         2               my-topic(0,1)

Проверить `BatchMessageConsumer`:

docker exec -it kafka-0 \
  kafka-consumer-groups.sh \
  --bootstrap-server kafka-0:9092 \
  --group batch-message-group \
  --describe --members --verbose


Пример результата:

docker exec -it kafka-0 \
  kafka-consumer-groups.sh \
  --bootstrap-server kafka-0:9092 \
  --group batch-message-group \
  --describe --members --verbose

GROUP               CONSUMER-ID                                  HOST            CLIENT-ID       #PARTITIONS     ASSIGNMENT
batch-message-group rdkafka-2490ab49-8ca2-4e99-847e-cab00b9f1e0b /172.18.0.7     rdkafka         2               my-topic(0,1)
batch-message-group rdkafka-f7e75439-d7b7-41e1-8c79-7193fe2f0026 /172.18.0.6     rdkafka         1               my-topic(2)


Таким образом Kafka автоматически распределяет три partitions между двумя Consumer внутри каждой группы

---

## Внутренний topic `__consumer_offsets`

Kafka использует внутренний topic `__consumer_offsets` для хранения offsets и служебной информации Consumer Groups

В кластере используется:

KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
KAFKA_CFG_MIN_INSYNC_REPLICAS: 2

Таким образом внутренний topic имеет три реплики и может продолжить работу при потере одного broker, пока в ISR остаётся минимум две реплики

---

## Docker

Для приложения используется образ:

FROM python:3.12-slim


В контейнер устанавливаются зависимости из `requirements.txt`

Основная зависимость:

confluent-kafka==2.15.1

После запуска контейнера выполняется:

python main.py


---

## Запуск проекта

### 1. Запустить Kafka-кластер

Сначала запускаются только Kafka и Kafka UI:

cd "1 module"

docker compose up -d kafka-0 kafka-1 kafka-2 kafka-ui


Проверить:

docker-compose up -d kafka-1 kafka-0 kafka-2 kafka-ui
[+] up 8/8
 ✔ Network kafka_default     Created                                                                                0.0s
 ✔ Volume kafka_kafka_2_data Created                                                                                0.0s
 ✔ Volume kafka_kafka_0_data Created                                                                                0.0s
 ✔ Volume kafka_kafka_1_data Created                                                                                0.0s
 ✔ Container kafka-2         Started                                                                                0.2s
 ✔ Container kafka-0         Started                                                                                0.2s
 ✔ Container kafka-1         Started                                                                                0.1s
 ✔ Container kafka-ui        Started                                                                                0.3s


Должны работать:

kafka-0
kafka-1
kafka-2
kafka-ui

### 2. Создать topic

docker exec -it kafka-0 \
  kafka-topics.sh \
  --create \
  --topic my-topic \
  --partitions 3 \
  --replication-factor 2 \
  --bootstrap-server kafka-0:9092

Проверить:

docker exec -it kafka-0 \
  kafka-topics.sh \
  --describe \
  --topic my-topic \
  --bootstrap-server kafka-0:9092

Ожидаемые параметры:

Topic: my-topic	TopicId: mvbF0CPqSxyUvFuPT9WNSQ	PartitionCount: 3	ReplicationFactor: 2	Configs: min.insync.replicas=2
	Topic: my-topic	Partition: 0	Leader: 1	Replicas: 1,2	Isr: 1,2
	Topic: my-topic	Partition: 1	Leader: 2	Replicas: 2,0	Isr: 2,0
	Topic: my-topic	Partition: 2	Leader: 0	Replicas: 0,1	Isr: 0,1

### 3. Собрать и запустить приложение

docker compose up -d --build kafka-app

Проверить контейнеры:

docker compose ps


Должны присутствовать два экземпляра:

kafka-kafka-app-1
kafka-kafka-app-2


### 4. Посмотреть логи приложения

docker compose logs -f kafka-app


В логах должны присутствовать:

docker compose logs -f kafka-app
kafka-app-1  | Kafka application запущено
kafka-app-1  | SingleMessageConsumer запущен...
kafka-app-1  | BatchMessageConsumer запущен...

kafka-app-2  | Kafka application запущено
kafka-app-2  | SingleMessageConsumer запущен...
kafka-app-2  | BatchMessageConsumer запущен...

Producer должен выводить отправляемые сообщения и информацию о доставке

Single Consumer должен выводить сообщения по одному

kafka-app-1  | Получено сообщение: id=34, text='Message number 34', key=34, partition=2, offset=16

kafka-app-2  | Получено сообщение: id=20, text='Message number 20', key=20, partition=0, offset=16

Batch Consumer должен выводить пачки:

Получена пачка: 10 сообщений
kafka-app-1  | Получена пачка: 10 сообщений
kafka-app-1  | Обрабатываем: id=1, text='Message number 1', key=1, partition=2, offset=0
kafka-app-1  | Обрабатываем: id=1, text='Message number 1', key=1, partition=2, offset=1
kafka-app-1  | Обрабатываем: id=8, text='Message number 8', key=8, partition=2, offset=2
kafka-app-1  | Обрабатываем: id=8, text='Message number 8', key=8, partition=2, offset=3
kafka-app-1  | Обрабатываем: id=13, text='Message number 13', key=13, partition=2, offset=4
kafka-app-1  | Обрабатываем: id=13, text='Message number 13', key=13, partition=2, offset=5
kafka-app-1  | Обрабатываем: id=17, text='Message number 17', key=17, partition=2, offset=6
kafka-app-1  | Обрабатываем: id=17, text='Message number 17', key=17, partition=2, offset=7
kafka-app-1  | Обрабатываем: id=22, text='Message number 22', key=22, partition=2, offset=8
kafka-app-1  | Обрабатываем: id=22, text='Message number 22', key=22, partition=2, offset=9

kafka-app-2  | Получена пачка: 10 сообщений
kafka-app-2  | Обрабатываем: id=2, text='Message number 2', key=2, partition=1, offset=0
kafka-app-2  | Обрабатываем: id=2, text='Message number 2', key=2, partition=1, offset=1
kafka-app-2  | Обрабатываем: id=3, text='Message number 3', key=3, partition=1, offset=2
kafka-app-2  | Обрабатываем: id=3, text='Message number 3', key=3, partition=1, offset=3
kafka-app-2  | Обрабатываем: id=4, text='Message number 4', key=4, partition=1, offset=4
kafka-app-2  | Обрабатываем: id=4, text='Message number 4', key=4, partition=1, offset=5
kafka-app-2  | Обрабатываем: id=5, text='Message number 5', key=5, partition=1, offset=6
kafka-app-2  | Обрабатываем: id=5, text='Message number 5', key=5, partition=1, offset=7
kafka-app-2  | Обрабатываем: id=6, text='Message number 6', key=6, partition=1, offset=8
kafka-app-2  | Обрабатываем: id=6, text='Message number 6', key=6, partition=1, offset=9

и после обработки:

kafka-app-1  | Batch из 10 сообщений обработан. Offset успешно закоммичен.

kafka-app-2  | Batch из 10 сообщений обработан. Offset успешно закоммичен.

### 5. Проверить Consumer Groups

Single Consumer:

docker exec -it kafka-0 \
  kafka-consumer-groups.sh \
  --bootstrap-server kafka-0:9092 \
  --group single-message-group \
  --describe --members --verbose

Batch Consumer:

docker exec -it kafka-0 \
  kafka-consumer-groups.sh \
  --bootstrap-server kafka-0:9092 \
  --group batch-message-group \
  --describe --members --verbose


В каждой группе должно быть 2 members, а partitions должны быть распределены между двумя Consumer.

---

## Kafka UI

Kafka UI доступен по адресу:


http://localhost:8080

В интерфейсе можно проверить:

- brokers;
- topics;
- partitions;
- messages;
- Consumer Groups;
- offsets;
- lag;
- состояние Consumer Group;

---

## Остановка проекта

Остановить контейнеры:

docker compose down


Полностью удалить контейнеры и Kafka volumes:


docker compose down -v

Важно: использование `-v` удаляет данные Kafka, включая `my-topic`, сообщения и consumer offsets 
После такого запуска topic необходимо создать заново

---

## Результат

В рамках практической работы реализовано:

- локальный Kafka-кластер из трёх узлов;
- KRaft без ZooKeeper;
- topic с 3 partitions и replication factor 2;
- Producer с JSON-сериализацией;
- At Least Once настройка Producer;
- `acks=all`;
- `retries=5`;
- `SingleMessageConsumer` с автоматическим commit offset;
- `BatchMessageConsumer` с batch минимум 10 сообщений;
- ручной synchronous commit после обработки batch;
- разные Consumer Groups;
- обработка ошибок;
- JSON-десериализация;
- параллельная работа Producer и Consumer;
- Docker-образ Python-приложения;
- запуск приложения в двух экземплярах;
- автоматическое распределение partitions между Consumer одной группы;