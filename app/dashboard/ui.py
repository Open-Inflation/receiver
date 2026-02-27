from __future__ import annotations

from pathlib import Path

from fastapi.responses import FileResponse

DASHBOARD_DIR = Path(__file__).resolve().parent
TEMPLATE_FILE = DASHBOARD_DIR / "templates" / "index.html"
VALIDATION_ERRORS_TEMPLATE_FILE = DASHBOARD_DIR / "templates" / "validation_errors.html"
ASSETS_DIR = DASHBOARD_DIR / "assets"


def dashboard_page_response() -> FileResponse:
    return FileResponse(TEMPLATE_FILE)


def validation_errors_page_response() -> FileResponse:
    return FileResponse(VALIDATION_ERRORS_TEMPLATE_FILE)
