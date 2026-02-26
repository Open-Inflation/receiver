from __future__ import annotations

import logging
from typing import Generator

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import Orchestrator

LOGGER = logging.getLogger(__name__)


def get_db_session(request: Request) -> Generator[Session, None, None]:
    session_factory = request.app.state.session_factory
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing Authorization header")

    prefix = "Bearer "
    if not authorization.startswith(prefix):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Bearer token is required")

    token = authorization[len(prefix) :].strip()
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Bearer token is empty")
    return token


def get_current_orchestrator(
    request: Request,
    authorization: str | None = Header(default=None),
    session: Session = Depends(get_db_session),
) -> Orchestrator:
    try:
        token = _extract_bearer_token(authorization)
    except HTTPException:
        LOGGER.warning(
            "Authorization failed: path=%s client=%s reason=invalid_authorization_header",
            request.url.path,
            request.client.host if request.client else None,
        )
        raise

    orchestrator = session.scalar(select(Orchestrator).where(Orchestrator.token == token))
    if orchestrator is None:
        LOGGER.warning(
            "Authorization failed: path=%s client=%s reason=unknown_token token_length=%s",
            request.url.path,
            request.client.host if request.client else None,
            len(token),
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid orchestrator token")
    LOGGER.debug("Authorization success: orchestrator_id=%s path=%s", orchestrator.id, request.url.path)
    return orchestrator
