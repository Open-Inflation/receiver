# Receiver Server

Сервис-обработчик для управления своими оркестраторами:
- выдача задач оркестраторам;
- прием результатов выполнения;
- отправка изображений на отдельный storage-сервер (`../storage`) без конвертации;
- сохранение ответа скрапера и метаданных архива в БД.

Конвертация изображений выполняется на стороне `storage`.

Используется SQLAlchemy ORM и MySQL. Для локальных тестов поддерживается SQLite.

## Что хранится в таблице задач

Таблица `crawl_tasks` включает обязательные поля:
- `city`
- `store`
- `last_crawl_at`
- `frequency_hours`

Дополнительно есть служебные поля lease/статуса для безопасной выдачи задач нескольким оркестраторам.

## Где сохраняется ответ скрапера

В таблицу `task_runs`:
- `payload_json` - исходный payload от оркестратора;
- `parser_payload_json` - нормализованный payload (через интеграцию с `../parser`, если доступна);
- `image_results_json` - результат загрузки изображений в storage (url/ошибки);
- `output_json`, `output_gz`, `download_url`, `download_sha256`, `download_expires_at` - артефакты готового архива;
- `status`, `error_message`, `finished_at`.

## Переменные окружения

- `DATABASE_URL` - URL БД, например:
  - MySQL: `mysql+pymysql://user:pass@127.0.0.1:3306/receiver`
  - SQLite (по умолчанию): `sqlite:///data/receiver.db`
- `STORAGE_BASE_URL` - base URL storage-сервера (`http://127.0.0.1:8000`)
- `STORAGE_API_TOKEN` - Bearer-токен для `POST /api/images` storage-сервера
- `PARSER_SRC_PATH` - путь к `../parser/src` для мягкой интеграции parser-модуля
- `LEASE_TTL_MINUTES` - время аренды задачи оркестратором (по умолчанию `30`)

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

## API

- `POST /api/orchestrators/register` - регистрация оркестратора (возвращает token)
- `POST /api/orchestrators/heartbeat` - heartbeat (Bearer token)
- `POST /api/orchestrators/next-task` - получить следующую просроченную задачу (Bearer token)
- `POST /api/orchestrators/results` - отправить результат run + артефакты архива + изображения (Bearer token)
- `POST /api/tasks` - создать задачу
- `GET /api/tasks` - список задач
- `PATCH /api/tasks/{task_id}` - обновить задачу
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
