# Test Payment Processing Service

Сервис принимает платеж, сохраняет его в PostgreSQL, публикует событие `payments.new` через outbox и отдельный dispatcher, затем consumer:

- имитирует обработку платежа внешним шлюзом
- обновляет статус платежа в БД
- отправляет webhook клиенту
- делает retry при временных ошибках
- отправляет сообщение в DLQ после исчерпания попыток

## Архитектура

### Основные компоненты

- `api` - эндпоинты для создания и получения платежа
- `outbox-dispatcher` - фоновый воркер, который читает таблицу `outbox` и публикует события в RabbitMQ
- `consumer` - воркер, который читает очередь `payments.new` и выполняет основную обработку
- `postgres` - хранение платежей и outbox-сообщений
- `rabbitmq` - транспорт для событий, retry-очередей и DLQ

### Флоу

1. `POST /api/v1/payments` создает платеж
2. В этой же транзакции создается запись в `outbox`
3. `outbox-dispatcher` публикует событие `payments.new` в RabbitMQ
4. `consumer` забирает сообщение из очереди `payments.new`
5. Consumer имитирует обработку 2-5 секунд
6. При успехе платеж получает статус `succeeded`
7. При финальной неудаче платеж получает статус `failed`, webhook отправляется с этим статусом, а сообщение уходит в DLQ
8. При временной ошибке сообщение переходит в retry-очередь

## Retry и DLQ

### Retry

Для временных ошибок consumer использует до 3 попыток:

- 1-я попытка: `payments.new`
- 2-я попытка: `payments.new.retry.1`
- 3-я попытка: `payments.new.retry.2`

Задержки между попытками экспоненциальные:

- retry 1: 2 секунды
- retry 2: 4 секунды

### DLQ

В DLQ сообщение попадает, если:

- consumer исчерпал 3 попытки
- webhook не удалось доставить после повторов
- сообщение повреждено или невалидно
- outbox не смог опубликовать событие после повторов

DLQ-очередь: `payments.new.dlq`

## Запуск

### Через Docker Compose

```bash
docker compose up --build
```

### Остановка

```bash
docker compose down
```

### Что будет доступно после старта

- API: `http://localhost:8013`
- Swagger: `http://localhost:8013/docs`
- RabbitMQ панель: `http://localhost:15672`

### Доступ в RabbitMQ

- user: `guest`
- password: `guest`

## Переменные окружения

- `API_KEY` - ключ для заголовка `X-API-Key`
- `POSTGRES_USER` - пользователь PostgreSQL
- `POSTGRES_PASSWORD` - пароль PostgreSQL
- `POSTGRES_DB` - имя базы данных
- `RABBITMQ_DEFAULT_USER` - пользователь RabbitMQ
- `RABBITMQ_DEFAULT_PASS` - пароль RabbitMQ
- `DB_URL` - async URL для SQLAlchemy
- `DB_URL_SYNC` - sync URL для Alembic
- `RABBIT_URL` - URL для RabbitMQ

Если переменные не заданы, `docker compose` подставляет значения по умолчанию

## API

Все запросы требуют заголовок:

```text
X-API-Key: 123
```

### Создание платежа

`POST /api/v1/payments`

Обязательные заголовки:

- `X-API-Key`
- `Idempotency-Key`

Пример:

```bash
curl -X POST http://localhost:8013/api/v1/payments/ \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: 123' \
  -H 'Idempotency-Key: 1' \
  -d '{
    "amount": "1000.00",
    "currency": "RUB",
    "description": "Оплата заказа 1",
    "metadata": {
      "order_id": 1
    },
    "webhook_url": "https://httpbin.org/status/200"
  }'
```

Пример ответа:

```json
{
  "payment_id": "58a5009e-e32c-44bc-ae05-6ff5ccaff4e5",
  "status": "pending",
  "created_at": "2026-07-23T01:02:54.537271Z"
}
```

### Повторный запрос с тем же `Idempotency-Key`

Если тело совпадает, сервис вернет уже созданный платеж:

```bash
curl -X POST http://localhost:8013/api/v1/payments/ \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: 123' \
  -H 'Idempotency-Key: 1' \
  -d '{
    "amount": "1000.00",
    "currency": "RUB",
    "description": "Оплата заказа 1",
    "metadata": {
      "order_id": 1
    },
    "webhook_url": "https://httpbin.org/status/200"
  }'
```

Если тело отличается, сервис вернет конфликт `409`

### Получение платежа

`GET /api/v1/payments/{payment_id}`

Пример:

```bash
curl http://localhost:8013/api/v1/payments/{uuid} \
  -H 'X-API-Key: 123'
```

Пример ответа:

```json
{
  "payment_id": "58a5009e-e32c-44bc-ae05-6ff5ccaff4e5",
  "amount": "1000.00",
  "currency": "RUB",
  "description": "Оплата заказа 1",
  "metadata": {
    "order_id": 1
  },
  "status": "succeeded",
  "webhook_url": "https://httpbin.org/status/200",
  "created_at": "2026-07-23T01:02:54.537271Z",
  "processed_at": "2026-07-23T01:02:58.671301Z"
}
```

## Возможные ответы API

### `202 Accepted`

Платеж создан и поставлен в обработку

### `401 Unauthorized`

Если не передан `X-API-Key` или ключ неверный

### `404 Not Found`

Если платежа с таким `payment_id` нет

### `409 Conflict`

Если `Idempotency-Key` уже использован с другим телом запроса

## Webhook

После обработки consumer отправляет POST на `webhook_url`

Пример payload:

```json
{
  "payment_id": "58a5009e-e32c-44bc-ae05-6ff5ccaff4e5",
  "status": "succeeded",
  "processed_at": "2026-07-23T01:02:58.671301Z"
}
```

Если платеж завершился неудачно на финальной попытке, payload будет таким же, но со статусом `failed`

## Очереди RabbitMQ

### Основные очереди

- `payments.new`
- `payments.new.retry.1`
- `payments.new.retry.2`
- `payments.new.dlq`

### Принцип работы

- `payments.new` получает новые события из outbox
- Если consumer падает на временной ошибке, сообщение перепубликовывается в retry-очередь
- После TTL сообщение возвращается обратно в `payments.new`
- После 3 неудачных попыток сообщение публикуется в `payments.new.dlq`

## Механизм работы

1. API сохраняет платеж и запись в `outbox` в одной транзакции
2. `outbox-dispatcher` забирает готовые записи и публикует событие `payments.new`
3. `consumer` получает сообщение и извлекает `payment_id`
4. Обработка имитирует задержку 2-5 секунд
5. В 90% случаев платеж успешно обрабатывается
6. В 10% случаев возникает временная ошибка
7. Временные ошибки повторяются через retry-очереди
8. После исчерпания попыток сообщение отправляется в DLQ
9. После успешной обработки клиент получает webhook
