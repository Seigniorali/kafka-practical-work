import faust


# Обычное сообщение между пользователями. user_id — отправитель, recipient_id — получатель.
class Message(faust.Record, serializer="json"):
    user_id: str
    recipient_id: str
    message: str
    timestamp: int


# Событие изменения списка блокировок. user_id — владелец blacklist, blocked_user_id — пользователь, которого блокируют/разблокируют.
class BlockEvent(faust.Record, serializer="json"):
    user_id: str
    blocked_user_id: str
    action: str


# Событие изменения динамического списка запрещённых слов. action принимает значения add или remove.
class ForbiddenWordEvent(faust.Record, serializer="json"):
    word: str
    action: str
