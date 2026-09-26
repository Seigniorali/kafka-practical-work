import os
import faust
from app.models import BlockEvent, ForbiddenWordEvent, Message


# При локальном запуске Faust подключается к Kafka через localhost:19094. В Docker Compose значение переопределяется на kafka://kafka-0:9092
KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "kafka://localhost:19094",
)


# RocksDB хранит локальное состояние Faust Tables на диске
app = faust.App(
    "messaging-service",
    broker=KAFKA_BROKER,
    store="rocksdb://",
)


# Входящие сообщения. Kafka key = recipient_id
messages_topic = app.topic(
    "messages",
    key_type=str,
    value_type=Message,
)

# Только сообщения, прошедшие blacklist и цензуру
filtered_messages_topic = app.topic(
    "filtered_messages",
    key_type=str,
    value_type=Message,
)

# События block/unblock. Kafka key = user_id владельца blacklist
blocked_users_topic = app.topic(
    "blocked_users",
    key_type=str,
    value_type=BlockEvent,
)

# События add/remove запрещённых слов. Kafka key = само слово
forbidden_words_topic = app.topic(
    "forbidden_words",
    key_type=str,
    value_type=ForbiddenWordEvent,
)


# Текущее состояние динамического списка запрещённых слов: слово -> True/False
forbidden_words_table = app.Table(
    "forbidden-words-table",
    default=bool,
    partitions=3,
    options={
        "driver": "rocksdict",
    },
)

# Текущее состояние blacklist: user_id -> список заблокированных пользователей
blocked_users_table = app.Table(
    "blocked-users-table",
    default=list,
    partitions=3,
    options={
        "driver": "rocksdict",
    },
)

#Маскирует все активные запрещённые слова символами '*'
def censor_message(text: str) -> str:
    censored_text = text

    for word, active in forbidden_words_table.items():
        if not active:
            continue

        censored_text = censored_text.replace(
            word,
            "*" * len(word),
        )

    return censored_text

#Проверяет blacklist, выполняет цензуру и отправляет результат дальше
@app.agent(messages_topic)
async def process_messages(stream):
    async for recipient_id, message in stream.items():

        # Kafka key должен совпадать с recipient_id внутри Message
        if recipient_id != message.recipient_id:
            print(
                f"Ошибка: Kafka key={recipient_id}, "
                f"но message.recipient_id={message.recipient_id}"
            )
            continue

        # Получаем blacklist именно получателя сообщения.
        blocked_users = blocked_users_table[recipient_id]

        # Если отправитель заблокирован — сообщение отбрасывается и до filtered_messages не доходит
        if message.user_id in blocked_users:
            print(
                f"Сообщение заблокировано: "
                f"{message.user_id} -> {message.recipient_id}"
            )
            continue

        # Все разрешённые сообщения проходят через цензуру
        censored_text = censor_message(message.message)

        filtered_message = Message(
            user_id=message.user_id,
            recipient_id=message.recipient_id,
            message=censored_text,
            timestamp=message.timestamp,
        )

        await filtered_messages_topic.send(
            key=recipient_id,
            value=filtered_message,
        )

        print(
            f"Сообщение разрешено: "
            f"{message.user_id} -> {message.recipient_id}, "
            f"message='{filtered_message.message}'"
        )

#Обрабатывает block/unblock и обновляет persistent Table
@app.agent(blocked_users_topic)
async def update_blocked_users(stream):
    async for user_id, event in stream.items():

        # Kafka key должен быть user_id владельца blacklist
        if user_id != event.user_id:
            print(
                f"Ошибка: Kafka key={user_id}, "
                f"но event.user_id={event.user_id}"
            )
            continue

        # set не допускает дублирования одного blocked_user_id
        blocked_users = set(blocked_users_table[user_id])

        if event.action == "block":
            blocked_users.add(event.blocked_user_id)

        elif event.action == "unblock":
            blocked_users.discard(event.blocked_user_id)

        else:
            print(
                f"Неизвестное действие блокировки: "
                f"{event.action}"
            )
            continue

        # Присваиваем новое значение, чтобы Faust сохранил изменение в RocksDB и changelog topic
        blocked_users_table[user_id] = sorted(blocked_users)

        print(
            f"Список блокировок пользователя {user_id}: "
            f"{blocked_users_table[user_id]}"
        )

#Обрабатывает add/remove и обновляет список запрещённых слов
@app.agent(forbidden_words_topic)
async def update_forbidden_words(stream):
    async for word, event in stream.items():

        # Нормализуем конфигурационное слово перед сохранением
        normalized_word = event.word.strip().lower()

        # Kafka key и слово в событии должны совпадать
        if word != normalized_word:
            print(
                f"Ошибка: Kafka key={word}, "
                f"но event.word={normalized_word}"
            )
            continue

        if event.action == "add":
            forbidden_words_table[normalized_word] = True

        elif event.action == "remove":
            forbidden_words_table[normalized_word] = False

        else:
            print(
                f"Неизвестное действие: {event.action}"
            )
            continue

        active_words = sorted(
            word
            for word, active in forbidden_words_table.items()
            if active
        )

        print(
            f"Активные запрещённые слова: {active_words}"
        )
