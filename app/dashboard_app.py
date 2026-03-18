from __future__ import annotations

import argparse
from contextlib import asynccontextmanager
import logging
import time
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.engine import make_url

from app.config import Settings, load_settings
from app.dashboard.routes import create_dashboard_router
from app.dashboard.ui import ASSETS_DIR, dashboard_page_response, validation_errors_page_response
from app.dashboard.utils import (
    dispatch_meta,
    ensure_sqlite_parent_dir,
    task_to_dict,
)
from app.database import create_session_factory, create_sqlalchemy_engine, ensure_task_runs_runtime_columns
from app.logging_utils import ensure_logging_configured
from app.models import Base

LOGGER = logging.getLogger(__name__)
ORCHESTRATOR_WS_MAX_MESSAGE_BYTES = 32 * 1024 * 1024


async def _connect_orchestrator_ws(ws_url: str) -> Any:
    try:
        import websockets
    except ModuleNotFoundError as exc:
        raise RuntimeError("Package 'websockets' is required for dashboard log proxy") from exc
    return await websockets.connect(
        ws_url,
        max_size=ORCHESTRATOR_WS_MAX_MESSAGE_BYTES,
    )

def _ensure_sqlite_parent_dir(database_url: str) -> None:
    ensure_sqlite_parent_dir(database_url)


def _get_orchestrator_ws_connector():
    return _connect_orchestrator_ws


def create_dashboard_app(settings: Settings | None = None) -> FastAPI:
    ensure_logging_configured()
    app_settings = settings or load_settings()
    safe_database_url = make_url(app_settings.database_url).render_as_string(hide_password=True)
    LOGGER.info(
        "Initializing dashboard app: db_url=%s ws_url=%s",
        safe_database_url,
        app_settings.orchestrator_ws_url,
    )

    _ensure_sqlite_parent_dir(app_settings.database_url)
    engine = create_sqlalchemy_engine(app_settings.database_url)
    session_factory = create_session_factory(engine)
    Base.metadata.create_all(bind=engine)
    ensure_task_runs_runtime_columns(engine)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            LOGGER.info("Disposing dashboard SQLAlchemy engine")
            engine.dispose()

    app = FastAPI(title="Receiver Dashboard", version="0.1.0", lifespan=lifespan)
    app.state.engine = engine
    app.state.session_factory = session_factory

    app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="dashboard-assets")

    @app.middleware("http")
    async def log_http_requests(request: Request, call_next):
        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            LOGGER.exception(
                "Dashboard HTTP %s %s failed in %.2fms",
                request.method,
                request.url.path,
                elapsed_ms,
            )
            raise

        elapsed_ms = (time.perf_counter() - start) * 1000.0
        LOGGER.info(
            "Dashboard HTTP %s %s -> %s in %.2fms",
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
        )
        return response

    @app.get("/", include_in_schema=False)
    def dashboard_page() -> FileResponse:
        LOGGER.debug("Dashboard page requested")
        return dashboard_page_response()

    @app.get("/validation-errors", include_in_schema=False)
    def validation_errors_page() -> FileResponse:
        LOGGER.debug("Dashboard validation errors page requested")
        return validation_errors_page_response()

    app.include_router(
        create_dashboard_router(
            session_factory=session_factory,
            app_settings=app_settings,
            connect_orchestrator_ws_getter=_get_orchestrator_ws_connector,
        )
    )
    return app


app = create_dashboard_app()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Receiver dashboard app")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8091)
    parser.add_argument("--reload", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    uvicorn.run("app.dashboard_app:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
