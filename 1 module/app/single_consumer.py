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


consumer_config = {
    "bootstrap.servers": KAFKA_SERVERS,                             # Kafka brokers, через которые Consumer подключается к кластеру.

    "group.id": "single-message-group",                             # идентификатор группы Consumer

    "auto.offset.reset": "earliest",                                # При не сохранённом offset, читать всё сначал topic

    "group.protocol": "classic",

    "enable.auto.commit": True,                                     # Auto commit offset 

    "auto.commit.interval.ms": 1000,                                # Периодичность как часто Kafka автоматически коммитит offset. 1000 мс = 1 секунда

    "session.timeout.ms": 10000,                                     # Время за которое Consumer должен ответить Kafka, иначе Kafka считает его недоступным. 10000 мс = 10 секунд
}


consumer = Consumer(consumer_config)

consumer.subscribe([TOPIC])                                         # Подписываю consmer на топик


def main():
    print("SingleMessageConsumer запущен...")

    try:
        while True:

            msg = consumer.poll(1.0)                                # Получаем сообщение от Kafka, ожидание 1 секунда

            if msg is None:                                         # Если нет сообщения, то продолжает
                continue


            if msg.error():
                print(f"Ошибка Consumer: {msg.error()}")            # Если есть ошибка, то выводим её в консоль
                continue

            try:
                message = deserialize_message(msg.value())          # Десериализация сообщения из JSON в объект Message

                key = (                                             # Если есть ключ сообщения, то декодируем его в строку, иначе None
                    msg.key().decode("utf-8")
                    if msg.key() is not None
                    else None
                )

                print(
                    f"Получено сообщение: "
                    f"id={message.id}, "
                    f"text='{message.text}', "
                    f"key={key}, "
                    f"partition={msg.partition()}, "
                    f"offset={msg.offset()}"
                )

            except Exception as error:                              # Ошибка десериализации сообщения, выводим её в консоль и продолжаем обработку следующих сообщений
                print(f"Ошибка обработки сообщения: {error}")
                continue

    except KeyboardInterrupt:                                       # Если пользователь нажал Ctrl+C, то выходим из цикла и завершаем работу Consumer
        print("\nОстановка SingleMessageConsumer...")

    finally:                                                        # Закрываем Consumer, чтобы освободить ресурсы и корректно завершить работу
        consumer.close()
        print("SingleMessageConsumer завершил работу.")


if __name__ == "__main__":                                          # Если этот файл запускается как основной, то вызываем функцию main() для запуска Consumer
    main()