# Receiver Server

Сервис-обработчик для управления своими оркестраторами:
- выдача задач оркестраторам;
- прием результатов выполнения;
- отправка изображений на отдельный storage-сервер (`../storage`) без конвертации;
- сохранение нормализованного артефакта в БД.

`receiver` только загружает изображения в storage (временные URL).
Перенос в permanent storage и замена URL выполняются только в `converter` при записи в целевую БД.
`receiver` автоматически отправляет задачи в оркестратор `parser` по WebSocket (`submit_store/status`), если включен bridge.

Используется SQLAlchemy ORM и PostgreSQL (`psycopg`). Для локальных тестов поддерживается SQLite.

## Что хранится в таблице задач

Таблица `crawl_tasks` включает обязательные поля:
- `city`
- `store`
- `last_crawl_at`
- `frequency_hours`
- `include_images` - запрашивать изображения карточек в `submit_store`
- `use_product_info` - включать обогащение карточек через `Catalog.Product.info` (fixprice/chizhik)

Дополнительно есть служебные поля lease/статуса для безопасной выдачи задач нескольким оркестраторам.

## Где сохраняется ответ скрапера

В таблицу `task_runs`:
- `dispatch_meta_json` - только служебные метаданные bridge (`remote_job_id`, служебные статусы);
- `processed_images` - количество успешно загруженных изображений;
- `status`, `error_message`, `finished_at`;
- `artifact_source`, `artifact_products_count`, `artifact_categories_count`,
  `artifact_dataclass_validated`, `artifact_dataclass_validation_error`, `artifact_ingested_at` -
  стабильный run-снимок для dashboard, независимый от дальнейшей очистки `run_artifacts*` converter-ом;
- `converter_elapsed_sec` - накопленное время (в секундах), которое `converter` потратил на перенос чанков этого run;
- `finish` - метка времени завершения consume-delete этим `converter`.

Детальный payload скрапера и сырой список загрузки изображений в `task_runs` больше не сохраняются.
Нормализованные данные сохраняются в `run_artifacts*`.
В `run_artifacts` хранится `parser_name` (какой парсер выполнил обход), сырой `payload_json` не хранится.

## Миграции

Для PostgreSQL runtime схема создается ORM-моделями (`Base.metadata.create_all`) и актуальными типами (`BIGINT/JSONB/ENUM/TIMESTAMPTZ/NUMERIC`).

## Переменные окружения

- `DATABASE_URL` - URL БД, например:
  - PostgreSQL: `postgresql+psycopg://user:pass@127.0.0.1:5432/receiver`
  - PostgreSQL (alias): `postgresql://user:pass@127.0.0.1:5432/receiver`
  - SQLite (по умолчанию): `sqlite:///data/receiver.db`
- `STORAGE_BASE_URL` - base URL storage-сервера (`http://127.0.0.1:8000`)
- `STORAGE_API_TOKEN` - Bearer-токен для `POST /api/images/{image_name}` storage-сервера
- `PARSER_SRC_PATH` - путь к `../parser/src` для мягкой интеграции parser-модуля
- `LEASE_TTL_MINUTES` - время аренды задачи оркестратором (по умолчанию `30`)
- `ORCHESTRATOR_MAX_CLAIMS_PER_CYCLE` - максимум новых задач за один цикл bridge (по умолчанию `2`)
- `ORCHESTRATOR_ASSIGNED_PARALLELISM` - параллелизм обработки `assigned` run в bridge (по умолчанию `2`)
- `ORCHESTRATOR_MAX_ASSIGNED_BACKLOG` - лимит числа `assigned` run; при достижении claim-фаза в цикле bridge пропускается (по умолчанию `1`)
- `ORCHESTRATOR_AUTO_DISPATCH_ENABLED` - включить авто-диспетчер в parser WebSocket (`true` по умолчанию)
- `ORCHESTRATOR_WS_URL` - адрес parser orchestrator WS (по умолчанию `ws://127.0.0.1:8765`)
- `ORCHESTRATOR_WS_PASSWORD` - пароль, если parser запущен с `--auth-password`
- `ORCHESTRATOR_POLL_INTERVAL_SEC` - интервал poll статуса задач (по умолчанию `5`)
- `ORCHESTRATOR_MANAGER_NAME` - имя внутреннего оркестратора в БД receiver (по умолчанию `parser-ws`)
- `ORCHESTRATOR_SUBMIT_INCLUDE_IMAGES` - дефолт `include_images` для новых задач (если в API/dashboard явно не задано)
- `ORCHESTRATOR_SUBMIT_USE_PRODUCT_INFO` - дефолт `use_product_info` для новых задач (если в API/dashboard явно не задано)
- `ORCHESTRATOR_UPLOAD_ARCHIVE_IMAGES` - после `success` загружать изображения из `output_gz` в storage (`true` по умолчанию)
- `ARTIFACT_DOWNLOAD_MAX_BYTES` - лимит размера скачиваемого артефакта по `download_url` (по умолчанию `268435456`)
- `ARTIFACT_JSON_MEMBER_MAX_BYTES` - лимит размера JSON файла внутри артефакта/`output_json` (по умолчанию `16777216`)
- `ARTIFACT_INGEST_PRODUCTS_PER_TXN` - число товаров в одной транзакции ingest (по умолчанию `200`)
- `ARTIFACT_INGEST_CATEGORIES_PER_TXN` - число категорий в одной транзакции ingest (по умолчанию `1000`)
- `ARTIFACT_INGEST_RELATIONS_PER_TXN` - число дочерних строк товаров (`meta/images/wholesale/category_links`) в одной транзакции ingest (по умолчанию `2000`)
- `IMAGE_ARCHIVE_MAX_FILE_BYTES` - лимит размера одной картинки в `images/` архива (по умолчанию `12582912`)
- `IMAGE_ARCHIVE_MAX_FILES` - лимит числа картинок в `images/` архива (по умолчанию `2000`)

## Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
pip install -e .[test]
```

## Запуск

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8090
```

## Отдельный фронт-скрипт

Есть отдельный исполняемый Python-скрипт `dashboard.py`.
Это отдельное web-приложение для:
- управления задачами (`create/update active/frequency/parser/include_images/use_product_info`);
- просмотра общей статистики (`tasks/runs/orchestrators`);
- просмотра последних запусков;
- отдельной страницы ошибок валидации, подлежащих устранению (`/validation-errors`).

Важно: дашборд не управляет `last_crawl_at`; это поле обновляет основной скрипт при сохранении результатов обхода.

Запуск:

```bash
./dashboard.py --host 127.0.0.1 --port 8091
```

или

```bash
python dashboard.py --host 127.0.0.1 --port 8091
```

Открыть в браузере:

`http://127.0.0.1:8091`

## API

- `POST /api/orchestrators/register` - регистрация оркестратора (возвращает token)
- `POST /api/orchestrators/heartbeat` - heartbeat (Bearer token)
- `POST /api/orchestrators/next-task` - получить следующую просроченную задачу (Bearer token)
- `POST /api/orchestrators/results` - отправить результат run + артефакты архива + изображения (Bearer token)
- `POST /api/tasks` - создать задачу
- `GET /api/tasks` - список задач
- `PATCH /api/tasks/{task_id}` - обновить задачу
- `GET /api/validation-errors` - список актуальных ошибок dataclass validation (dashboard; по последнему успешному run каждой задачи)
- `GET /api/runs/{run_id}` - получить run
- `GET /healthz` - healthcheck

## Формат отправки результата

`POST /api/orchestrators/results`

```json
{
  "run_id": "...",
  "status": "success",
  "payload": {"any": "json"},
  "images": [
    {
      "filename": "shelf.jpg",
      "content_base64": "..."
    }
  ],
  "upload_images_from_archive": true,
  "output_json": "/abs/path/store_20260225.json",
  "output_gz": "/abs/path/store_20260225.tar.gz",
  "download_url": "http://127.0.0.1:8766/download?...",
  "download_sha256": "...",
  "download_expires_at": "2026-02-25T12:00:00+00:00"
}
```

- `images[]` - прямые изображения для отправки в storage;
- `upload_images_from_archive=true` - дополнительно взять файлы из `images/` внутри `output_gz` и отправить в storage.

## Тесты

```bash
pytest
```
