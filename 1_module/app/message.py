import json
from dataclasses import dataclass


@dataclass
class Message:
    id: int
    text: str


def deserialize_message(data: bytes) -> Message:
    try:
        json_string = data.decode("utf-8")                  # Kafka возвращает value в виде bytes. Превращаем bytes в обычную строку


        message_dict = json.loads(json_string)              # JSON-строку превращаем в Python dictionary

        return Message(**message_dict)                      # Из dictionary создаём объект Message.

    except Exception as error:
        print(f"Ошибка десериализации: {error}")
        raise