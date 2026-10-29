"""Configured-contact privacy over real worker retries, error storage, and logging seams."""

import json
import logging
import os
from contextlib import closing
from datetime import timedelta
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl, quote, urlsplit

import httpx2
import pytest
import stamina

from kittum.app import (
    JOB_HANDLERS,
    AppContext,
    build_gateway,
    create_project,
    ingest_corpus,
    job_handlers,
    request_enrichment,
)
from kittum.app.open_access import queue_retrieval
from kittum.core import FixedClock, SystemClock, new_id, sha256_hex
from kittum.corpus import latest_revision, reports
from kittum.db import Database
from kittum.gateway import CONTACT_PLACEHOLDER, HttpRequest, HttpxTransport
from kittum.ledger import PostgresLedger, SigningKey, seal, verify_bundle
from kittum.runs import JobOutput, JobSpec, PermanentJobError, PostgresJobQueue, Worker
from kittum.search import SearchPlan, import_file
from kittum.settings import Settings
from kittum.storage import FilesystemStore, MemoryStore
from tests.app.test_contact_privacy import AUTHOR, CONTACT, RIS, _decoded
from tests.documents.conftest import savepoint_db

if TYPE_CHECKING:
    from pathlib import Path

    from kittum.core import Clock, Id
    from kittum.db import Tx
    from kittum.storage import ArtifactStore

pytestmark = pytest.mark.postgres


def _project(
    pg: Tx, artifacts: ArtifactStore, ledger: PostgresLedger, clock: Clock
) -> tuple[Id, Id, Id]:
    pg.execute("update job set run_after = now() + interval '1 day' where status = 'queued'")
    pid, lead = create_project(
        pg, name="Synthetic contact retry", lead_name="Synthetic lead", ledger=ledger
    )
    import_file(
        pg,
        pid,
        SearchPlan(
            topic_id="synthetic",
            source="citation_search",
            platform="synthetic",
            strategy="synthetic",
        ),
        RIS.encode(),
        "ris",
        ledger=ledger,
        actor_id=lead,
        artifacts=artifacts,
        clock=clock,
    )
    ingest_corpus(pg, pid, actor_id=lead, ledger=ledger)
    revision = latest_revision(pg, pid)
    assert revision is not None
    return pid, lead, reports(pg, pid, revision.id)[0].report_id


@pytest.mark.parametrize("interruption", ["source_unavailable", "worker_crash"])
def test_a_fresh_worker_rebuilds_the_request_from_settings_without_expanding_audit_urls(
    database: Database, tmp_path: Path, caplog: pytest.LogCaptureFixture, interruption: str
) -> None:
    class SimulatedCrash(BaseException):
        pass

    after_restart = "issue74.restarted+synthetic@example.invalid"
    seen: list[httpx2.Request] = []
    recovering = False

    def peer(request: httpx2.Request) -> httpx2.Response:
        seen.append(request)
        if not recovering:
            if interruption == "worker_crash":
                raise SimulatedCrash()
            return httpx2.Response(503, json={"error": f"synthetic outage: {request.url}"})
        return httpx2.Response(200, json={"records": []})

    artifacts = FilesystemStore(tmp_path / "artifacts")
    clock = FixedClock(SystemClock().now())
    ledger, queue = PostgresLedger(clock), PostgresJobQueue(clock)
    with database.transaction() as tx:
        pid, lead, report_id = _project(tx, artifacts, ledger, clock)
        run_id = queue_retrieval(
            tx,
            pid,
            [report_id],
            actor_id=lead,
            trigger="requested",
            ledger=ledger,
            queue=queue,
            clock=clock,
        )
    with httpx2.Client(transport=httpx2.MockTransport(peer)) as client:
        settings = Settings(database_url="unused", artifact_dir=tmp_path, contact_email=CONTACT)
        gateway = build_gateway(
            settings,
            database,
            artifacts,
            SystemClock(),
            transport=HttpxTransport(contact_email=CONTACT, client=client),
        )
        ctx = AppContext(
            settings=settings,
            db=database,
            artifacts=artifacts,
            clock=clock,
            gateway=gateway,
            ledger=ledger,
            queue=queue,
        )
        worker = Worker(
            database, queue, job_handlers(ctx), name="before-restart", lease=timedelta(minutes=5)
        )
        with (
            stamina.set_testing(True, attempts=10, cap=True),
            caplog.at_level(logging.INFO, logger="httpx2"),
        ):
            if interruption == "worker_crash":
                with pytest.raises(SimulatedCrash):
                    worker.run_once()
            else:
                assert worker.run_once()
        gateway.close()
    with database.transaction() as tx:
        before = ledger.events(tx, pid)
        row = tx.execute(
            "select * from retrieval_run_report where run_id = %s", (run_id,)
        ).fetchone()
    original_hashes = [e.event_hash for e in before]
    assert row is not None
    if interruption == "source_unavailable":
        assert CONTACT_PLACEHOLDER in row["attempts"][0]["url"]
        assert row["outcome"] == "not_acquired"
    else:
        assert row["attempts"] == [] and row["outcome"] is None
    assert CONTACT.casefold() not in _decoded(json.dumps(row, default=str))
    assert all(dict(parse_qsl(str(r.url.query.decode())))["email"] == CONTACT for r in seen)
    first_count = len(seen)

    # Read committed state through a new pool, store, ledger, queue, context and worker, after
    # the old lease expires. No savepoint or application object carries the old request across.
    recovering = True
    clock.advance(timedelta(minutes=6))
    with (
        closing(Database(os.environ["KITTUM_DATABASE_URL"], max_size=4)) as restarted,
        httpx2.Client(transport=httpx2.MockTransport(peer)) as client,
    ):
        artifacts = FilesystemStore(tmp_path / "artifacts")
        ledger, queue = PostgresLedger(clock), PostgresJobQueue(clock)
        if interruption == "source_unavailable":
            with restarted.transaction() as tx:
                run_id = queue_retrieval(
                    tx,
                    pid,
                    [report_id],
                    actor_id=lead,
                    trigger="retry",
                    ledger=ledger,
                    queue=queue,
                    clock=clock,
                )
        settings = Settings(
            database_url="unused", artifact_dir=tmp_path, contact_email=after_restart
        )
        gateway = build_gateway(
            settings,
            restarted,
            artifacts,
            SystemClock(),
            transport=HttpxTransport(contact_email=after_restart, client=client),
        )
        ctx = AppContext(
            settings=settings,
            db=restarted,
            artifacts=artifacts,
            clock=clock,
            gateway=gateway,
            ledger=ledger,
            queue=queue,
        )
        worker = Worker(
            restarted, queue, job_handlers(ctx), name="after-restart", lease=timedelta(minutes=5)
        )
        with caplog.at_level(logging.INFO, logger="httpx2"):
            assert worker.run_once()
        assert not worker.run_once()
        gateway.close()
        with restarted.transaction() as tx:
            row = tx.execute(
                "select * from retrieval_run_report where run_id = %s", (run_id,)
            ).fetchone()
            assert [
                e.event_hash for e in ledger.events(tx, pid, through_seq=before[-1].seq)
            ] == original_hashes
            assert ledger.verify(tx, pid).ok
            events = ledger.events(tx, pid)
            bundle, signer = tmp_path / "restarted-bundle", SigningKey.generate()
            seal(
                tx,
                pid,
                events[-1].seq,
                artifacts,
                bundle,
                signer=signer,
                actor_id=lead,
                ledger=ledger,
            )
    assert len(seen) == first_count + 1
    assert dict(parse_qsl(urlsplit(str(seen[-1].url)).query))["email"] == after_restart
    assert after_restart in seen[-1].headers["User-Agent"]
    assert row is not None and row["outcome"] == "no_location"
    assert row["attempts"][0]["url"].endswith("email=" + CONTACT_PLACEHOLDER)
    assert verify_bundle(bundle, trusted_key=signer.verify_key).ok
    stored = (
        json.dumps(row, default=str)
        + json.dumps([e.model_dump(mode="json") for e in events])
        + caplog.text
    )
    stored += "".join(
        p.read_bytes().decode(errors="ignore") for p in bundle.rglob("*") if p.is_file()
    )
    assert all(address.casefold() not in _decoded(stored) for address in (CONTACT, after_restart))


def test_source_logging_is_scoped_and_does_not_rebind_an_arbitrary_url_or_change_content(
    database: Database, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    seen: list[httpx2.Request] = []
    payload = json.dumps({"author": AUTHOR, "content": CONTACT}).encode()
    url = (
        "https://papers.example.invalid/report.pdf?email=contact%40example.invalid&cursor=a%2Bb&cursor=c&author="
        + AUTHOR
    )

    def peer(request: httpx2.Request) -> httpx2.Response:
        seen.append(request)
        logging.getLogger("httpcore2.connection").debug("diagnostic %s", CONTACT)
        logging.getLogger("stamina").warning(
            "retry",
            extra={
                "stamina.args": (repr(request), CONTACT),
                "stamina.kwargs": {"request": request, "contact": CONTACT},
            },
        )
        if request.url.path == "/error":
            return httpx2.Response(400, json={"error": CONTACT + " " + AUTHOR})
        return httpx2.Response(200, content=payload)

    artifacts = MemoryStore()
    settings = Settings(database_url="unused", artifact_dir=tmp_path, contact_email=CONTACT)
    with httpx2.Client(transport=httpx2.MockTransport(peer)) as client:
        gateway = build_gateway(
            settings,
            database,
            artifacts,
            SystemClock(),
            transport=HttpxTransport(contact_email=CONTACT, client=client),
        )
        with caplog.at_level(logging.DEBUG):
            result = gateway.fetch("crossref", HttpRequest(url=url))
            error = gateway.fetch(
                "crossref", HttpRequest(url="https://papers.example.invalid/error")
            )
        gateway.close()
    assert result.body == payload and result.body_hash == sha256_hex(payload)
    assert CONTACT.encode() not in error.body and AUTHOR.encode() in error.body
    assert error.body_hash == sha256_hex(error.body)
    assert parse_qsl(urlsplit(str(seen[0].url)).query) == parse_qsl(urlsplit(url).query)
    assert dict(parse_qsl(urlsplit(str(seen[0].url)).query))["email"] == CONTACT_PLACEHOLDER
    assert dict(parse_qsl(urlsplit(str(seen[0].url)).query))["author"] == AUTHOR
    active_logs = caplog.text + json.dumps([r.__dict__ for r in caplog.records], default=str)
    assert CONTACT.casefold() not in _decoded(active_logs)
    # Filters are inert outside this app gateway's context, including for the same logger.
    caplog.clear()
    logging.getLogger("httpx2").warning("unrelated request %s", CONTACT)
    assert CONTACT in caplog.text


def test_enrichment_failure_rows_and_their_ledger_events_are_safe(
    pg: Tx, database: Database, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    seen: list[httpx2.Request] = []

    def peer(request: httpx2.Request) -> httpx2.Response:
        seen.append(request)
        if request.url.host == "api.unpaywall.org":
            raise OSError(1, f"synthetic transport failure for {request.url}")
        return httpx2.Response(404)

    db, artifacts, clock = savepoint_db(pg), MemoryStore(), SystemClock()
    ledger, queue = PostgresLedger(clock), PostgresJobQueue(clock)
    pid, lead, _ = _project(pg, artifacts, ledger, clock)
    settings = Settings(database_url="unused", artifact_dir=tmp_path, contact_email=CONTACT)
    with httpx2.Client(transport=httpx2.MockTransport(peer)) as client:
        gateway = build_gateway(
            settings,
            database,
            artifacts,
            clock,
            transport=HttpxTransport(contact_email=CONTACT, client=client),
        )
        ctx = AppContext(
            settings=settings,
            db=db,
            artifacts=artifacts,
            clock=clock,
            gateway=gateway,
            ledger=ledger,
            queue=queue,
        )
        run = request_enrichment(
            pg, pid, actor_id=lead, trigger="requested", ledger=ledger, queue=queue, clock=clock
        )
        worker = Worker(db, queue, job_handlers(ctx), name="safe-enrichment")
        with caplog.at_level(logging.INFO, logger="httpx2"):
            assert worker.run_once()
        gateway.close()
    assert any(
        request.url.host == "api.unpaywall.org"
        and dict(parse_qsl(urlsplit(str(request.url)).query))["email"] == CONTACT
        for request in seen
    )
    rows = pg.execute("select * from enrichment_run_record where run_id = %s", (run.id,)).fetchall()
    assert any("synthetic transport failure" in str(row["failed"]) for row in rows)
    stored = (
        json.dumps(rows, default=str)
        + json.dumps([e.model_dump(mode="json") for e in ledger.events(pg, pid)])
        + caplog.text
    )
    assert CONTACT.casefold() not in _decoded(stored)
    assert ledger.verify(pg, pid).ok


@pytest.mark.parametrize("failure", ["handler", "publish", "permanent", "readonly", "unconfigured"])
def test_worker_failure_storage_and_tracebacks_do_not_keep_the_contact(
    pg: Tx,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    class ReadOnlyFailure(RuntimeError):
        @property
        def reason(self) -> str:
            return f"operator={CONTACT}"

        def __str__(self) -> str:
            return self.reason

    db, artifacts, clock = savepoint_db(pg), MemoryStore(), SystemClock()
    ledger, queue = PostgresLedger(clock), PostgresJobQueue(clock)
    pid, _ = create_project(
        pg, name="Synthetic worker failure", lead_name="Synthetic lead", ledger=ledger
    )
    settings = Settings(
        database_url="unused",
        artifact_dir=tmp_path,
        contact_email=CONTACT if failure != "unconfigured" else None,
    )
    ctx = AppContext(settings=settings, db=db, artifacts=artifacts, ledger=ledger, queue=queue)

    def fail() -> None:
        worker.stop()
        if failure == "unconfigured":
            raise Exception(ValueError("ordinary"))
        try:
            raise OSError(1, f"cause={quote(CONTACT, safe='')}")
        except OSError as cause:
            error = (
                ReadOnlyFailure()
                if failure == "readonly"
                else PermanentJobError(f"operator={CONTACT}")
                if failure == "permanent"
                else RuntimeError(f"operator={CONTACT}")
            )
            error.add_note(f"note={CONTACT.upper()}")
            raise error from cause

    def handle(_job, _job_ctx):
        if failure == "publish":
            return JobOutput(publish=lambda _tx: fail())
        fail()

    monkeypatch.setitem(JOB_HANDLERS, "synthetic.contact.failure", lambda _ctx: handle)
    pg.execute("update job set run_after = now() + interval '1 day' where status = 'queued'")
    job = queue.enqueue(
        pg,
        JobSpec(
            project_id=pid, kind="synthetic.contact.failure", stage_key=str(new_id()), payload={}
        ),
    )
    worker = Worker(db, queue, job_handlers(ctx), name="safe-failure")
    with caplog.at_level(logging.INFO, logger="kittum.runs.worker"):
        worker.run()
    row = pg.execute("select * from job where id = %s", (job.job_id,)).fetchone()
    assert row is not None
    if failure == "unconfigured":
        assert row["last_error"] == "Exception: ordinary"
        return
    if failure == "publish":
        assert row["status"] == "running"  # unchanged: publishing rolled back its transaction
    else:
        assert row["last_error"] and CONTACT_PLACEHOLDER in row["last_error"]
        assert row["status"] == ("failed" if failure == "permanent" else "queued")
    stored = json.dumps(row, default=str) + caplog.text
    assert CONTACT.casefold() not in _decoded(stored)
    if failure not in ("permanent", "readonly"):
        assert "cause=" + CONTACT_PLACEHOLDER in caplog.text
        assert "note=" + CONTACT_PLACEHOLDER in caplog.text
