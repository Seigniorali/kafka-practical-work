import threading
import time

from producer import main as producer_main
from single_consumer import main as single_consumer_main
from batch_consumer import main as batch_consumer_main


def main():
    print("Kafka application запущено")


    single_thread = threading.Thread(                       # Создаём отдельные потоки для двух Consumer.
        target=single_consumer_main,
        name="single-consumer",
        daemon=True,
    )

    batch_thread = threading.Thread(
        target=batch_consumer_main,
        name="batch-consumer",
        daemon=True,
    )


    single_thread.start()                                   # Сначала запускаем Consumer.
    batch_thread.start()


    time.sleep(5)                                           # Даём Consumer немного времени подключиться к Kafka и получить свои partitions.


    producer_main()                                         # Producer отправляет сообщения.

    print("Producer завершил отправку сообщений.")
    print("Consumers продолжают работу...")


    try:                                                    # После завершения работы Producer, оставляем приложение работать, чтобы Consumer продолжали получать сообщения
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("Остановка приложения...")


if __name__ == "__main__":
    main()