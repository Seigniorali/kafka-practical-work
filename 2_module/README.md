# Practical Work 3 — Kafka + Faust Messaging Service

Упрощённый сервис обмена сообщениями на Python/Faust с persistent state

## Что реализовано

Сервис выполняет две проверки входящих сообщений:

1. **Blacklist пользователей** — если отправитель находится в списке блокировок получателя, сообщение отбрасывается и не попадает в `filtered_messages`
2. **Динамическая цензура** — все разрешённые сообщения проходят через список запрещённых слов; активные слова заменяются символами `*`

Состояние хранится в Faust Tables на RocksDB и восстанавливается через Kafka changelog topics.

## Архитектура

blocked_users -> update_blocked_users() -> blocked_users_table
                                             |
messages -> process_messages() ---------------+
              |
              | sender blocked? -> yes -> DROP
              |
              v
         censor_message()
              ^
              |
     forbidden_words_table
              ^
              |
forbidden_words -> update_forbidden_words()
              |
              v
       filtered_messages

## Kafka topics

| Topic | Назначение |
|---|---|
| `messages` | Входящие сообщения пользователей |
| `filtered_messages` | Сообщения, прошедшие blacklist и цензуру |
| `blocked_users` | События `block` / `unblock` |
| `forbidden_words` | События `add` / `remove` запрещённых слов |
| `messaging-service-blocked-users-table-changelog` | Changelog blacklist Table |
| `messaging-service-forbidden-words-table-changelog` | Changelog forbidden words Table |

## Модели

`Message`:

```json
{"user_id":"user-1","recipient_id":"user-2","message":"Hello","timestamp":1727001001}
```

- `user_id` — отправитель;
- `recipient_id` — получатель;
- Kafka key для `messages` равен `recipient_id`.

`BlockEvent`:

```json
{"user_id":"user-2","blocked_user_id":"user-4","action":"block"}
```

- `user_id` — владелец blacklist;
- `blocked_user_id` — кого блокируют/разблокируют;
- Kafka key равен `user_id`.

`ForbiddenWordEvent`:

```json
{"word":"stupid","action":"add"}
```

Kafka key равен самому слову.

## Структура проекта

```text
2_module/
├── app/
│   ├── __init__.py
│   ├── main.py
│   └── models.py
├── .dockerignore
├── docker-compose.yml
├── Dockerfile
├── README.md
└── requirements.txt
```

## Запуск

Если локальный Faust worker уже запущен, сначала остановите его, чтобы одновременно не работали две копии приложения.

```bash
docker compose up --build -d
```

Проверить контейнеры:

```bash
docker compose ps
```

Логи Faust:

```bash
docker compose logs -f messaging-service
```

Kafka UI: `http://localhost:18080`

## End-to-end тест

### 1. Заблокировать user-4 для user-2

```bash
docker compose exec kafka-0 \
  kafka-console-producer.sh \
  --bootstrap-server kafka-0:9092 \
  --topic blocked_users \
  --property parse.key=true \
  --property key.separator=:
```

Отправить:

user-2:{"user_id":"user-2","blocked_user_id":"user-4","action":"block"}

Ожидаемый лог:

Список блокировок пользователя user-2: ['user-4']


### 2. Добавить запрещённое слово

```bash
docker compose exec kafka-0 \
  kafka-console-producer.sh \
  --bootstrap-server kafka-0:9092 \
  --topic forbidden_words \
  --property parse.key=true \
  --property key.separator=:
```

Отправить:

stupid:{"word":"stupid","action":"add"}

### 3. Отправить разрешённое сообщение

```bash
docker compose exec kafka-0 \
  kafka-console-producer.sh \
  --bootstrap-server kafka-0:9092 \
  --topic messages \
  --property parse.key=true \
  --property key.separator=:
```

user-2:{"user_id":"user-1","recipient_id":"user-2","message":"Hello friend","timestamp":1727001001}

Сообщение должно попасть в `filtered_messages` без изменений.

### 4. Проверить цензуру

user-2:{"user_id":"user-1","recipient_id":"user-2","message":"You are stupid","timestamp":1727001002}

Ожидаемый результат:

You are ******


### 5. Проверить блокировку

user-2:{"user_id":"user-4","recipient_id":"user-2","message":"You are stupid","timestamp":1727001003}


Ожидаемый лог:

Сообщение заблокировано: user-4 -> user-2

Эта запись не должна появиться в `filtered_messages`.

### 6. Разблокировать user-4

В producer топика `blocked_users`:

user-2:{"user_id":"user-2","blocked_user_id":"user-4","action":"unblock"}

После этого сообщение от `user-4` снова проходит, но `stupid` всё ещё маскируется.

### 7. Удалить запрещённое слово

В producer топика `forbidden_words`:

stupid:{"word":"stupid","action":"remove"}


После этого:

user-2:{"user_id":"user-4","recipient_id":"user-2","message":"You are stupid","timestamp":1727001005}

должно попасть в `filtered_messages` без маскировки.

## Просмотр результата

```bash
docker compose exec kafka-0 \
  kafka-console-consumer.sh \
  --bootstrap-server kafka-0:9092 \
  --topic filtered_messages \
  --property print.key=true \
  --property key.separator=:
```

## Persistent state

Faust использует RocksDB Tables:

blocked_users_table
forbidden_words_table

В Docker локальное состояние хранится в named volume `faust_data`. Kafka changelog topics позволяют восстановить Tables при потере локального RocksDB state.

Обычная остановка сохраняет данные:

```bash
docker compose down
```

Полное удаление Kafka и Faust volumes:

```bash
docker compose down -v
```

## Локальный запуск Faust

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
faust -A app.main worker -l INFO
```

Локально используется `kafka://localhost:19094`; внутри Compose переменная `KAFKA_BROKER` переопределяется на `kafka://kafka-0:9092`.