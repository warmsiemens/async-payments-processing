# Асинхронный процессинг платежей

FastAPI + Pydantic v2, асинхронный SQLAlchemy 2, PostgreSQL, RabbitMQ через FastStream,
Alembic и Docker Compose. Python 3.12. `main.py` — точка входа API.

## Запуск

Запустите Docker Desktop / Docker Engine. В PowerShell:

```powershell
Copy-Item .env.example .env
docker compose --profile demo up -d --build
docker compose ps
```

В Linux/macOS вместо `Copy-Item` используйте `cp .env.example .env`.
Ключ из примера: `local-test-api-key-change-me`; его можно изменить в `.env`.
Миграции выполняет одноразовый сервис `migrate`; API, consumer и outbox стартуют после их завершения.

| Сервис | Адрес на хосте |
| --- | --- |
| API | http://localhost:8000 |
| PostgreSQL | localhost:15432, БД/логин/пароль: `payments` |
| RabbitMQ AMQP | localhost:5672 |
| RabbitMQ Management | http://localhost:15672, логин/пароль: `payments` |
| Демо webhook, профиль `demo` | http://localhost:8080/webhook |

Без демонстрационного получателя: `docker compose up -d --build`.
Порт PostgreSQL на хосте настраивается через `POSTGRES_PORT`; внутри Docker используется `postgres:5432`.
Сервисы внутри Docker обращаются друг к другу по именам, поэтому webhook в примере имеет
адрес `http://webhook:8080/webhook`. `localhost` в контейнере означает сам контейнер.
Для получателя на Windows/macOS-хосте используйте `http://host.docker.internal:<port>/<path>`.

```powershell
docker compose logs -f api consumer outbox webhook
docker compose down
```

`down` сохраняет данные в именованных томах.

## API

Все HTTP-пути платежного API, включая `/health`, `/docs`, `/redoc`, `/openapi.json`
и неизвестные пути, требуют `X-API-Key`. Отсутствующий или неверный ключ: `401`.
Swagger/OpenAPI также защищены: для браузера понадобится расширение, добавляющее этот заголовок
к запросам на `localhost:8000`; затем в Swagger нажмите **Authorize** и укажите тот же ключ.
Демонстрационный webhook — отдельное приложение клиента.

### Создание платежа

```powershell
$headers = @{
    'X-API-Key' = 'local-test-api-key-change-me'
    'Idempotency-Key' = 'order-42'
}
$body = @{
    amount = '120.50'
    currency = 'RUB'
    description = 'Order #42'
    metadata = @{ order_id = 42 }
    webhook_url = 'http://webhook:8080/webhook'
} | ConvertTo-Json -Depth 10

$payment = Invoke-RestMethod -Method Post -Uri 'http://localhost:8000/api/v1/payments' `
    -Headers $headers -ContentType 'application/json' -Body $body
$payment
```

Для curl (Bash):

```bash
curl -i http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: local-test-api-key-change-me' \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"120.50","currency":"RUB","description":"Order #42","metadata":{"order_id":42},"webhook_url":"http://webhook:8080/webhook"}'
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "356ec25b-6f9c-43d7-aab8-3a14c0e97c7e",
  "status": "pending",
  "created_at": "2026-10-14T10:00:00Z"
}
```

`amount` — положительный `decimal`, максимум 16 цифр до точки и 2 после неё.
Суммы возвращаются строкой для сохранения точности. Валюты: `RUB`, `USD`, `EUR`.
`description` обязателен, до 1000 символов; `metadata` — JSON-объект, по умолчанию `{}`;
`webhook_url` — HTTP(S) URL без встроенного логина/пароля.
`Idempotency-Key` обязателен, 1–255 символов без пробелов.
Ошибки валидации: `422`.

Повтор с тем же ключом и тем же содержимым возвращает `202`, исходный ID, исходное время
и текущий статус, без нового платежа или события. Разные представления суммы `120.5`
и `120.50` считаются одинаковыми. Тот же ключ с другим содержимым: `409 Conflict`.
Конкурентные запросы защищены уникальным ограничением PostgreSQL и `ON CONFLICT`.

### Получение платежа

```powershell
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/payments/$($payment.payment_id)" `
    -Headers @{ 'X-API-Key' = 'local-test-api-key-change-me' }
```

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> \
  -H 'X-API-Key: local-test-api-key-change-me'
```

Ответ `200` содержит `payment_id`, сумму, валюту, описание, метаданные, статус,
`idempotency_key`, `webhook_url`, `created_at`, `processed_at`, `webhook_delivered_at`.
Неизвестный UUID: `404`; неверный формат ID: `422`.
Все даты содержат часовой пояс UTC.

## Обработка, outbox и гарантии

```mermaid
flowchart LR
    Client -->|POST + два ключа| API
    API -->|одна транзакция| DB[(payments + outbox)]
    DB --> Publisher[Outbox publisher]
    Publisher -->|publisher confirm| MQ[payments.new]
    MQ --> Consumer[Один consumer]
    Consumer --> Gateway[Эмуляция шлюза]
    Consumer -->|сохранение результата| DB
    Consumer -->|HTTP POST| Webhook[Webhook клиента]
    Consumer -->|retry или DLQ одной транзакцией| DB
    Publisher --> DLQ[payments.dlq]
```

1. API атомарно сохраняет платеж `pending` и событие в `outbox`. Для приема платежа
   доступность RabbitMQ не требуется.
2. Отдельный сервис `outbox` выбирает готовые события через `FOR UPDATE SKIP LOCKED`,
   фиксирует временное владение событием и закрывает транзакцию, затем публикует
   persistent-сообщение с `mandatory=True` и подтверждениями RabbitMQ.
   `published_at` сохраняется только после подтверждения. Ошибки публикации повторяются
   с экспоненциальной задержкой до 60 секунд без ограничения количества попыток.
3. Единственный обработчик `consumer` слушает `payments.new`, `prefetch=1`.
   Эмуляция шлюза занимает 2–5 секунд: вероятность `succeeded` — 90%, `failed` — 10%.
   Отказ шлюза — завершенный платеж, о котором также отправляется webhook.
4. Результат шлюза и `processed_at` коммитятся **до** HTTP-запроса. Повторные попытки
   отправки webhook используют сохраненный результат. Ошибка webhook не меняет
   успешный платеж на `failed`.
5. У шлюза и webhook отдельные бюджеты: **3 попытки** каждой операции, исходная и две
   повторные, с задержками **2 и 4 секунды** по умолчанию. Событие содержит `stage`
   (`processing` или `webhook`), а платеж хранит `processing_attempts` и `webhook_attempts`.
   Consumer атомарно отмечает текущее событие обработанным
   и создает новое событие outbox с `available_at`. Задержка переживает рестарт процессов.
6. После третьей ошибки consumer атомарно создает DLQ-событие через outbox.
   Сообщение в `payments.dlq` содержит платеж, исходное событие, этап, число попыток и причину.
   Если шлюз исчерпал попытки, платеж становится `failed`, а в той же транзакции создается
   событие для отправки webhook с собственными тремя попытками. Ошибки программы, например
   `TypeError`, отправляются в DLQ сразу; traceback остается в журнале, в БД — краткая причина.
   Если даже сохранение retry/DLQ невозможно из-за недоступной БД, исходная доставка
   получает `nack(requeue=True)`: сообщение сохраняется, а попытка будет продолжена
   после восстановления БД.
7. Некорректные события, включая поврежденный JSON и UTF-8, отклоняются в dead-letter exchange.
   JSON разбирается внутри обработчика, поэтому ошибка декодирования не блокирует следующие сообщения.
   Используются durable
   direct-обменники `payments` и `payments.dead`, quorum-очереди `payments.new` и
   `payments.dlq`. Для `payments.new` включена доставка dead-letter **at least once**.
   `x-delivery-limit=-1` отключает встроенный лимит повторных доставок quorum-очереди:
   число попыток контролируется записью outbox, а недоступность БД не исчерпывает бюджет.

Между подтверждением публикации и фиксацией `published_at` возможен сбой, поэтому
доставка — **at least once**. Consumer проверяет `event_id` и `consumed_at` под блокировкой
строки; повторные сообщения не создают дополнительный retry и используют сохраненный результат шлюза.
Consumer и publisher используют временное владение с уникальным токеном; шлюз, HTTP и RabbitMQ
вызываются без открытых сессий и блокировок БД. `WORKER_LEASE_SECONDS` по умолчанию равен 60,
минимум — больше 10 секунд. Перед внешней операцией lease consumer обновляется, а сама операция
ограничена половиной срока lease; для HTTP и публикации также действует соответствующий timeout.
При остановке worker задача становится доступной после истечения lease. Устаревший worker
не может зафиксировать результат поверх результата нового владельца.
Если процесс упал после вызова шлюза до сохранения результата, эмуляция может повториться;
для настоящего платежного шлюза потребуется его собственный idempotency key.
Таблица outbox также хранит отметки потребления, поэтому эти записи нельзя удалять
без согласованной политики хранения и дедупликации.

Webhook считается доставленным при `2xx`; перенаправления не выполняются.
Сетевые ошибки, timeout и остальные HTTP-статусы запускают retry.
Если клиент принял webhook, но ответ потерялся или процесс упал до коммита, уведомление
может прийти повторно. Получатель должен сохранять и проверять стабильный заголовок
`Idempotency-Key: payment:<payment_id>:processed`.

Пример webhook:

```json
{
  "event": "payment.processed",
  "payment_id": "356ec25b-6f9c-43d7-aab8-3a14c0e97c7e",
  "status": "succeeded",
  "amount": "120.50",
  "currency": "RUB",
  "description": "Order #42",
  "metadata": {"order_id": 42},
  "created_at": "2026-10-14T10:00:00+00:00",
  "processed_at": "2026-10-14T10:00:03+00:00"
}
```

## Проверка retry и DLQ

Создайте платеж с новым `Idempotency-Key` и
`webhook_url: "http://webhook:8080/webhook?status_code=503"`.
Демонстрационный получатель будет возвращать `503`. После трех попыток сообщение
появится в `payments.dlq` в RabbitMQ Management → Queues → `payments.dlq`.
Финальный статус платежа сохранится; `webhook_delivered_at` останется `null`.

```powershell
docker compose logs consumer outbox webhook
docker compose exec postgres psql -U payments -d payments -c "SELECT id, status, processing_attempts, webhook_delivered_at, last_error FROM payments;"
```

Проверка outbox при недоступном брокере: остановите `rabbitmq`, создайте платеж,
затем запустите брокер снова. API вернет `202`; неопубликованное событие останется в БД
и будет доставлено после восстановления подключения. Для рестарта приложения используются
политика `restart: unless-stopped` и robust-подключение RabbitMQ.

## Тесты

Полный набор с PostgreSQL и RabbitMQ в контейнерах:

```powershell
docker compose --profile test run --build --rm tests
```

Каждый тест использует отдельную случайную схему БД, а тесты RabbitMQ — отдельные очереди
и обменники. Данные работающего приложения не очищаются.
Проверяются аутентификация, валидация, чтение платежа, одновременные запросы с одним ключом,
атомарность создания, дедупликация сообщений, сохранение результата до callback,
retry, DLQ, persistent-публикация с подтверждением и отказ публикации без подходящей очереди.
Дополнительно проверяются поврежденные сообщения через настоящий consumer FastStream,
уведомление после окончательной ошибки шлюза, отдельные бюджеты retry, восстановление lease,
защита от устаревшего worker, отсутствие блокировок во время сети и upgrade/downgrade миграций.

Локальные проверки (активируйте интерпретатор проекта):

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q
python -m ruff check .
python -m ruff format --check .
```

Без `TEST_DATABASE_URL` интеграционные тесты БД пропускаются; без `TEST_RABBITMQ_URL`
пропускаются проверки настоящего брокера. Для полного локального прогона:

```powershell
docker compose up -d postgres rabbitmq
$env:TEST_DATABASE_URL = 'postgresql+asyncpg://payments:payments@localhost:15432/payments'
$env:TEST_RABBITMQ_URL = 'amqp://payments:payments@localhost:5672/'
python -m pytest -q
```

## Запуск без контейнеров приложения

После установки зависимостей, создания `.env` и запуска PostgreSQL/RabbitMQ:

```powershell
python -m alembic upgrade head
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

В отдельных терминалах:

```powershell
python -m app.workers.consumer
python -m app.workers.outbox
```

`python main.py` также запускает API.
Настройки читаются из переменных окружения и `.env`; переменные окружения имеют приоритет.
`API_KEY` обязателен и должен содержать минимум 16 символов. `RETRY_BASE_DELAY`,
`OUTBOX_POLL_INTERVAL`, `PUBLISH_TIMEOUT`, `WEBHOOK_TIMEOUT` задаются в секундах.
`WORKER_LEASE_SECONDS` задает срок владения задачей. Миграция `0002` добавляет поля lease
и счетчик webhook; события старого формата без `stage` поддерживаются.
Демо-вероятность и задержка шлюза фиксированы по заданию.

## Структура

```text
main.py                         Точка входа API
app/
├── application.py              Создание FastAPI, middleware, routers, lifecycle
├── api/
│   ├── dependencies.py         Получение БД и сессии через Depends, APIKeyHeader
│   ├── security.py             Проверка ключа с постоянным временем сравнения
│   ├── middleware.py           Авторизация всех HTTP-путей
│   ├── health.py               Проверка доступности БД
│   ├── router.py               Подключение маршрутов /api/v1
│   └── v1/payments.py          HTTP-эндпоинты и преобразование ошибок в ответы
├── core/
│   ├── config.py               Настройки окружения
│   ├── enums.py                Статусы платежа
│   ├── exceptions.py           Ошибки бизнес-логики без зависимости от FastAPI
│   ├── constants.py            Лимит попыток обработки
│   └── time.py                 Время UTC
├── db/
│   ├── base.py                 DeclarativeBase для ORM и Alembic
│   └── session.py              AsyncEngine и фабрика AsyncSession
├── models/
│   ├── __init__.py             Регистрация всех ORM-моделей для Alembic
│   ├── payment.py              SQLAlchemy-модель платежа
│   └── outbox.py               SQLAlchemy-модель outbox
├── schemas/
│   ├── payment.py              Валидация HTTP-запросов и схемы ответов
│   └── events.py               Контракт сообщения очереди
├── services/
│   ├── payments.py             Идемпотентное создание платежа и события
│   ├── events.py               Создание записей outbox
│   ├── processing.py           Шлюз, результат, webhook, retry/DLQ
│   └── outbox.py               Публикация события и сохранение подтверждения
├── messaging/
│   ├── broker.py               Создание подключения RabbitMQ
│   ├── constants.py            Имена очередей и routing keys
│   └── topology.py             Очереди, обменники, DLQ и bindings
└── workers/
    ├── consumer.py             Точка входа FastStream, один subscriber и ack/nack
    └── outbox.py               Точка входа и цикл публикации outbox
migrations/                     Асинхронные миграции Alembic
examples/webhook_receiver.py     Демо-клиент
tests/                          Unit- и интеграционные проверки
```

HTTP-слой отвечает за заголовки, зависимости, валидацию и коды ответа. Бизнес-логика
находится в `services`, модели БД и схемы API разделены: `models` описывает хранение,
`schemas` — входные и выходные данные. Общие типы из `core` не зависят от ORM или FastAPI.
`messaging` описывает RabbitMQ, а `workers` собирает зависимости и запускает процессы.
Фабрика `create_app` в `application.py` связывает настройки, БД и маршруты и позволяет
подставлять зависимости в тестах. API получает сессию через `Depends`; граница транзакции
создания платежа остается явной и включает запись outbox.

Для добавления ORM-модели создайте модуль в `models` и импортируйте его в `models/__init__.py`,
чтобы Alembic увидел таблицу. После этого создайте миграцию через `alembic revision --autogenerate`.

Документация библиотек: [FastStream RabbitBroker](https://faststream.ag2.ai/latest/public_api/faststream/rabbit/RabbitBroker/),
[quorum-очереди RabbitMQ](https://www.rabbitmq.com/docs/quorum-queues),
[подтверждения доставки RabbitMQ](https://www.rabbitmq.com/docs/confirms).
