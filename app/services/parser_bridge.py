from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path
from typing import Any, Callable


LOGGER = logging.getLogger(__name__)


class ParserBridge:
    """Soft integration with ../parser.

    If openinflation_parser is unavailable, the receiver still works.
    """

    def __init__(self, *, parser_src_path: Path):
        self._parse_request: Callable[[dict[str, Any]], Any] | None = None
        self._normalize_city_id: Callable[[Any], Any] | None = None
        self._enabled = False

        if not parser_src_path.exists():
            LOGGER.warning("Parser source path does not exist: %s", parser_src_path)
            return

        parser_src = str(parser_src_path)
        if parser_src not in sys.path:
            sys.path.insert(0, parser_src)

        try:
            requests_module = importlib.import_module("openinflation_parser.orchestration.requests")
            models_module = importlib.import_module("openinflation_parser.orchestration.models")
            self._parse_request = getattr(requests_module, "parse_request", None)
            self._normalize_city_id = getattr(models_module, "normalize_city_id", None)
            self._enabled = bool(self._parse_request or self._normalize_city_id)
        except Exception as exc:
            LOGGER.warning("Parser package import failed: %s", exc)

        if not self._enabled:
            LOGGER.warning("Parser integration disabled")
        else:
            LOGGER.info("Parser integration enabled from path: %s", parser_src_path)

    @property
    def enabled(self) -> bool:
        return self._enabled

    def normalize_payload(self, payload: dict[str, Any] | None) -> dict[str, Any] | None:
        if payload is None:
            return None

        normalized = dict(payload)

        if self._normalize_city_id is not None and "city_id" in normalized:
            try:
                normalized["city_id"] = self._normalize_city_id(normalized.get("city_id"))
            except Exception as exc:
                normalized["_city_id_normalization_error"] = str(exc)
                LOGGER.debug("city_id normalization failed: %s", exc)

        action = normalized.get("action")
        if self._parse_request is not None and isinstance(action, str):
            try:
                parsed = self._parse_request(normalized)
                if hasattr(parsed, "model_dump"):
                    normalized["_parser_request"] = parsed.model_dump(mode="json")
                else:
                    normalized["_parser_request"] = str(parsed)
            except Exception as exc:
                normalized["_parser_request_error"] = str(exc)
                LOGGER.debug("parse_request failed: %s", exc)

        return normalized
