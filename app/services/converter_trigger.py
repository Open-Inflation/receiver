from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass
from functools import lru_cache
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ConverterTriggerSettings:
    url: str | None
    token: str | None
    timeout_sec: float
    receiver_db: str | None
    catalog_db: str | None
    batch_size: int | None
    max_batches: int | None

    @property
    def enabled(self) -> bool:
        return self.url is not None


@lru_cache(maxsize=1)
def load_converter_trigger_settings() -> ConverterTriggerSettings:
    url = _safe_str(os.getenv("CONVERTER_TRIGGER_URL"))
    token = _safe_str(os.getenv("CONVERTER_TRIGGER_TOKEN"))
    receiver_db = _safe_str(os.getenv("CONVERTER_TRIGGER_RECEIVER_DB"))
    catalog_db = _safe_str(os.getenv("CONVERTER_TRIGGER_CATALOG_DB"))

    timeout_sec = _parse_float(os.getenv("CONVERTER_TRIGGER_TIMEOUT_SEC"), default=3.0, minimum=0.5)
    batch_size = _parse_int(os.getenv("CONVERTER_TRIGGER_BATCH_SIZE"), minimum=1)
    max_batches = _parse_int(os.getenv("CONVERTER_TRIGGER_MAX_BATCHES"), minimum=0)

    return ConverterTriggerSettings(
        url=url,
        token=token,
        timeout_sec=timeout_sec,
        receiver_db=receiver_db,
        catalog_db=catalog_db,
        batch_size=batch_size,
        max_batches=max_batches,
    )


def notify_converter_run_finished(*, run_id: str, parser_name: str) -> None:
    settings = load_converter_trigger_settings()
    if not settings.enabled:
        return

    payload: dict[str, object] = {
        "run_id": run_id,
        "parser_name": parser_name,
        "source": "receiver",
    }
    if settings.receiver_db is not None:
        payload["receiver_db"] = settings.receiver_db
    if settings.catalog_db is not None:
        payload["catalog_db"] = settings.catalog_db
    if settings.batch_size is not None:
        payload["batch_size"] = settings.batch_size
    if settings.max_batches is not None:
        payload["max_batches"] = settings.max_batches

    thread = threading.Thread(
        target=_post_trigger,
        args=(settings, payload),
        name="converter-trigger",
        daemon=True,
    )
    thread.start()


def _post_trigger(settings: ConverterTriggerSettings, payload: dict[str, object]) -> None:
    if settings.url is None:
        return

    request = Request(
        settings.url,
        data=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        method="POST",
    )
    request.add_header("Content-Type", "application/json")
    if settings.token is not None:
        request.add_header("Authorization", f"Bearer {settings.token}")

    try:
        with urlopen(request, timeout=settings.timeout_sec) as response:
            code = int(response.status)
            body = response.read().decode("utf-8", errors="replace")
            if code >= 400:
                LOGGER.warning(
                    "Converter trigger returned HTTP %s body=%s payload=%s",
                    code,
                    body,
                    payload,
                )
                return
            LOGGER.info("Converter trigger sent: code=%s payload=%s", code, payload)
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        LOGGER.warning(
            "Converter trigger HTTPError: code=%s reason=%s body=%s payload=%s",
            exc.code,
            exc.reason,
            body,
            payload,
        )
    except URLError as exc:
        LOGGER.warning("Converter trigger URLError: reason=%s payload=%s", exc.reason, payload)
    except Exception as exc:  # pragma: no cover
        LOGGER.warning("Converter trigger unexpected error: %s payload=%s", exc, payload)


def _safe_str(value: object | None) -> str | None:
    if value is None:
        return None
    token = str(value).strip()
    return token or None


def _parse_int(value: object | None, *, minimum: int) -> int | None:
    token = _safe_str(value)
    if token is None:
        return None
    try:
        parsed = int(token)
    except ValueError:
        return None
    return max(minimum, parsed)


def _parse_float(value: object | None, *, default: float, minimum: float) -> float:
    token = _safe_str(value)
    if token is None:
        return max(minimum, default)
    try:
        parsed = float(token)
    except ValueError:
        return max(minimum, default)
    return max(minimum, parsed)
