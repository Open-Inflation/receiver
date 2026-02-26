from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.database import Base, create_session_factory, create_sqlalchemy_engine
from app.models import CrawlTask, Orchestrator
from app.services.scheduler import claim_next_due_task, utcnow


def test_claim_next_due_task_tolerates_naive_lease_until(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'scheduler.sqlite3'}"
    engine = create_sqlalchemy_engine(db_url)
    session_factory = create_session_factory(engine)
    Base.metadata.create_all(bind=engine)

    seed_session = session_factory()
    try:
        now = utcnow()
        orchestrator = Orchestrator(
            id="o" * 32,
            name="orch-scheduler",
            token="t" * 40,
            created_at=now,
            updated_at=now,
            last_heartbeat_at=now,
        )
        task = CrawlTask(
            city="Moscow",
            store="C100",
            frequency_hours=1,
            parser_name="fixprice",
            is_active=True,
            created_at=now,
            updated_at=now,
            # Intentionally naive: this can exist with SQLite and previously
            # triggered SQLAlchemy evaluator sync crashes during UPDATE.
            last_crawl_at=(datetime.now(timezone.utc) - timedelta(hours=2)).replace(tzinfo=None),
            lease_until=(datetime.now(timezone.utc) - timedelta(minutes=5)).replace(tzinfo=None),
        )
        seed_session.add(orchestrator)
        seed_session.add(task)
        seed_session.commit()
    finally:
        seed_session.close()

    session = session_factory()
    try:
        orchestrator = session.get(Orchestrator, "o" * 32)
        assert orchestrator is not None

        claimed = claim_next_due_task(
            session,
            orchestrator=orchestrator,
            lease_ttl_minutes=30,
        )
        assert claimed is not None
        claimed_task, run = claimed
        assert claimed_task.id > 0
        assert run.task_id == claimed_task.id
        assert run.status == "assigned"
    finally:
        session.close()
        engine.dispose()
