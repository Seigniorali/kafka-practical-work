import os
import json 
import time
from dataclasses import asdict
from confluent_kafka import Producer
from message import Message

# Через os.getenv() для возможности менять хосты через переменные окружения
KAFKA_SERVERS = os.getenv(
    "KAFKA_SERVERS",
    "localhost:9094,localhost:9095,localhost:9096",                     # Указываем хосты Kafka, через которые Producer подключается к кластеру
)

TOPIC = os.getenv(                                                      # Указываем топик в который будем отправлять сообщения
    "TOPIC",
    "my-topic",
) 

MESSAGE_COUNT = int(
    os.getenv("MESSAGE_COUNT", "20")
)

#Конфиг продюсера Kafka
producer_config = { 
    "bootstrap.servers": KAFKA_SERVERS,                                 # Хосты Kafka
    "acks": "all",                                                      # Ждёт подтверждения от всех реплик об успкешной записи
    "retries": 5                                                        # Попытки отправки сообщения продюсером при ошибке 
}


producer = Producer(producer_config)                                    #Создаём продюсера с конифигурацией выше


#Функция для отображения статуса записи продюсером в топик
def delivery_report(err, msg): 
    if err is not None:                                                 # Если есть ошибка, то выводим её в консоль
        print(f"Ошибка доставки сообщения: {err}")                      # Вывод ошибки
    else:                                                               # Если нет ошибки, то выводим информацию о доставке
        print(
            f"Сообщение доставлено: "
            f"topic={msg.topic()}, "
            f"partition={msg.partition()}, "
            f"offset={msg.offset()}"
        )


# Серилизация сообщения в словарь и из словаря в JSON
def serialize_message(message: Message) -> str: 
    try:
        message_dict = asdict(message)                                  # Переводим сообещение в словарь
        return json.dumps(message_dict)                                 # Переводим словарь в JSON
    except Exception as error: 
        print(f"Ошибка сериализации: {error}")
        raise


def main ():
    try: 
        for message_id in range(1, MESSAGE_COUNT + 1):                                # Цикл для отправки 20 сообщений в топик Kafka
            message = Message( 
                id=message_id,
                text=f"Message number {message_id}",
            )

            serialized_message = serialize_message(message)             # Вызов сериализация сообщения в JSON
            print(f"Отправляем: {serialized_message}")

            producer.produce(                                           # Что продюсер отправляет в топик Kafka
                topic=TOPIC,
                key=str(message.id),
                value=serialized_message,
                on_delivery=delivery_report,
            )

            producer.poll(0)                                            # Вызов poll() для отработки событий 
            time.sleep(0.5)                                             # Задержка между отправкой сообщений в пол секунды для отображения всего процесса 

    except Exception as error:                                          # Ошибки Producer, если они возникнут
        if error is not None:
            print(f"Ошибка Producer: {error}")

    finally:                                                            # Вызов flush() для ожидания отправки всех сообщений перед завершением работы продюсера
        print("Ожидаем отправки оставшихся сообщений...")
        producer.flush()
        print("Producer завершил работу.")

if __name__ == "__main__":                                              
    main()