import os
from confluent_kafka import Consumer

from message import deserialize_message


# Через os.getenv() для возможности менять хосты через переменные окружения
KAFKA_SERVERS = os.getenv(
    "KAFKA_SERVERS",
    "localhost:9094,localhost:9095,localhost:9096",                     # Указываем хосты Kafka, через которые Producer подключается к кластеру
)

TOPIC = os.getenv(                                                      # Указываем топик в который будем отправлять сообщения
    "TOPIC",
    "my-topic",
) 

BATCH_SIZE = 10


consumer_config = {
    "bootstrap.servers": KAFKA_SERVERS,

    "group.id": "batch-message-group",                              # Указываю другую группу Consumer, чтобы offset не пересекался с single_consumer.py

    "auto.offset.reset": "earliest",

    "group.protocol": "classic",

    "enable.auto.commit": False,                                    # Отключаем автоматический commit offset, чтобы мы сами контролировали когда коммитить offset после обработки batch сообщений

    "session.timeout.ms": 10000,


    "fetch.min.bytes": 1,                                           # Broker старается накопить некоторое количество данных, # прежде чем вернуть fetch-ответ Consumer.

    "fetch.wait.max.ms": 5000,                                      # Максимальное время ожидания fetch-запроса в миллисекундах. Если данных меньше fetch.min.bytes, то broker будет ждать пока не накопится больше данных или не истечёт fetch.wait.max.ms.
}


consumer = Consumer(consumer_config)

consumer.subscribe([TOPIC])

def main():
    print("BatchMessageConsumer запущен...")                        # Вывод запуска BatchMessageConsumer в консоль

    try:
        while True:                                                 # Основной цикл, который будет выполняться до тех пор, пока не будет прерван пользователем (Ctrl+C)

            batch = []                                              # Лист для хранения сообщений, которые будут обрабатываться в batch, чтобы быть уверенным и собрать минимум BATCH_SIZE сообщений перед обработкой

            while len(batch) < BATCH_SIZE:                          # Пока количество сообщений в batch меньше BATCH_SIZE, продолжаем получать сообщения от Kafka

                messages = consumer.consume(
                    num_messages=BATCH_SIZE - len(batch),
                    timeout=5.0,
                )

                if not messages:                                    # Если не получено сообщений, то продолжаем цикл, чтобы снова попытаться получить сообщения
                    continue

                for msg in messages:                                # Если есть ошибка в сообщении, то выводим её в консоль и продолжаем цикл, чтобы обработать остальные сообщения

                    if msg.error():
                        print(f"Ошибка Consumer: {msg.error()}")
                        continue

                    batch.append(msg)                               # Добавляем сообщение в batch, чтобы потом обработать его вместе с другими сообщениями

            print(              
                f"\nПолучена пачка: "                               # Выводим в консоль информацию о том, что получена пачка сообщений и сколько сообщений в ней(в нашем случае BATCH_SIZE = 10, но может быть меньше, если в топике меньше сообщений)
                f"{len(batch)} сообщений"
            )

            for msg in batch:                                       # Обрабатываем каждое сообщение в batch, десериализуем его и выводим информацию о нём в консоль. Если есть ошибка при десериализации, то выводим её в консоль и продолжаем обработку остальных сообщений

                try:
                    message = deserialize_message(msg.value())

                    key = (
                        msg.key().decode("utf-8")                   # Если есть ключ сообщения, то декодируем его в строку, иначе None
                        if msg.key() is not None
                        else None
                    )

                    print(
                        f"Обрабатываем: "                           # Вывод информации
                        f"id={message.id}, "
                        f"text='{message.text}', "
                        f"key={key}, "
                        f"partition={msg.partition()}, "
                        f"offset={msg.offset()}"
                    )

                except Exception as error:                          # Ошибка десериализации сообщения, выводим её в консоль и продолжаем обработку следующих сообщений
                    print(
                        f"Ошибка обработки сообщения: {error}"
                    )


            consumer.commit(asynchronous=False)                     # Один commit после обработки всей пачки (в нашем случае 10 штук)

            print(
                f"Batch из {len(batch)} сообщений обработан. "
                f"Offset успешно закоммичен."
            )

    except KeyboardInterrupt:
        print("\nОстановка BatchMessageConsumer...")                # При ручной остановки вывод о завершении работы консюмера

    finally:
        consumer.close()                                            # Закрываем Consumer, чтобы освободить ресурсы и корректно завершить работу
        print("BatchMessageConsumer завершил работу.")

if __name__ == "__main__":
    main()