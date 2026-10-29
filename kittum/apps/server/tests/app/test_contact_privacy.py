"""Issue #74 acceptance probes: synthetic live requests, real rows, signed handover.

The configured contact stays on the wire, while lookup audit data and diagnostics are safe to
hand over. No source or model is contacted. Provider-private URL parameters are not expanded.
"""

import json
import logging
from datetime import timedelta
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl, quote, unquote, urlsplit

import httpx2
import pymupdf
import pytest
import stamina

from kittum.app import (
    AppContext,
    build_gateway,
    create_project,
    ingest_corpus,
    job_handlers,
)
from kittum.app.open_access import queue_retrieval
from kittum.core import SystemClock
from kittum.corpus import latest_revision, reports
from kittum.gateway import HttpxTransport
from kittum.ledger import PostgresLedger, SigningKey, seal, verify_bundle
from kittum.runs import PostgresJobQueue, Worker
from kittum.search import SearchPlan, import_file
from kittum.settings import Settings
from kittum.storage import MemoryStore
from tests.documents.conftest import savepoint_db

if TYPE_CHECKING:
    from pathlib import Path

    from kittum.db import Database, Tx

CONTACT = "issue74.operator+synthetic@example.invalid"
AUTHOR = "scholarly.author@example.invalid"
DOI = "10.5555/issue74-synthetic"
PDF_URL = "https://pmc-oa-opendata.s3.amazonaws.com/pdf/PMC999999999.1.pdf"
RIS = f"TY  - JOUR\nID  - synthetic\nTI  - Synthetic privacy probe\nDO  - {DOI}\nAB  - Author contact: {AUTHOR}\nER  - \n"


def _decoded(text: str) -> str:
    # Decode more than once so nested percent encodings cannot satisfy the privacy assertion.
    while True:
        decoded = unquote(text)
        if decoded == text:
            return text.casefold()
        text = decoded


@pytest.mark.postgres
@pytest.mark.parametrize(
    "answer",
    [
        "pdf",
        "none",
        "http_error",
        "transport_error",
        "redirect",
        "encoded_error",
        "truncated_error",
        "escaped_truncated_error",
        "recovered",
        "malformed",
    ],
)
def test_live_retrieval_keeps_contact_on_wire_and_out_of_rows_bundle_and_logs(
    pg: Tx, database: Database, tmp_path: Path, caplog: pytest.LogCaptureFixture, answer: str
) -> None:
    seen: list[httpx2.Request] = []
    pdf = pymupdf.open()
    pdf.new_page().insert_text((72, 72), f"Synthetic privacy probe\n{DOI}")
    pdf_bytes = pdf.tobytes()
    pdf.close()

    def source(request: httpx2.Request) -> httpx2.Response:
        seen.append(request)
        url = str(request.url)
        if "idconv" in url:
            if answer == "redirect" and not request.url.path.endswith("redirected"):
                target = (
                    url.split("?", 1)[0] + "redirected?tool=kittum&EMAIL=" + quote(CONTACT, safe="")
                )
                return httpx2.Response(302, headers={"Location": target})
            if answer == "transport_error":
                raise httpx2.ConnectError(f"synthetic failure for {url}", request=request)
            if answer in (
                "http_error",
                "encoded_error",
                "truncated_error",
                "escaped_truncated_error",
            ) or (answer == "recovered" and len(seen) < 3):
                message = f"synthetic outage for {url}"
                if answer == "encoded_error":
                    message = (
                        "duplicate ?%65MAIL="
                        + quote(quote(CONTACT.upper(), safe=""), safe="")
                        + "&email="
                        + CONTACT
                    )
                elif answer in ("truncated_error", "escaped_truncated_error"):
                    message = "x" * 280 + CONTACT
                if answer == "escaped_truncated_error":
                    return httpx2.Response(
                        503, content=json.dumps({"error": message}).replace("@", r"\u0040").encode()
                    )
                return httpx2.Response(503, json={"error": message})
            if answer == "malformed":
                return httpx2.Response(200, content=b"not JSON")
            return httpx2.Response(
                200,
                json={"records": [] if answer == "none" else [{"pmcid": "PMC999999999"}]},
            )
        if "list-type" in url:
            return httpx2.Response(
                200,
                content=b"<ListBucketResult><Key>metadata/PMC999999999.1.json</Key></ListBucketResult>",
            )
        if "metadata/PMC999999999.1.json" in url:
            return httpx2.Response(
                200,
                json={
                    "doi": DOI,
                    "version": 1,
                    "license_code": "CC-BY",
                    "pdf_url": "s3://pmc-oa-opendata/pdf/PMC999999999.1.pdf",
                },
            )
        if url == PDF_URL:
            return httpx2.Response(
                200, content=pdf_bytes, headers={"Content-Type": "application/pdf"}
            )
        return httpx2.Response(404)

    db, artifacts, clock = savepoint_db(pg), MemoryStore(), SystemClock()
    ledger, queue = PostgresLedger(clock), PostgresJobQueue(clock)
    settings = Settings(database_url="unused", artifact_dir=tmp_path, contact_email=CONTACT)
    # This is the live transport and composition root. Only its network peer is a fake.
    with httpx2.Client(transport=httpx2.MockTransport(source), follow_redirects=True) as client:
        transport = HttpxTransport(contact_email=CONTACT, client=client)
        # Gateway bookkeeping commits independently, as it does in a live worker. The app's
        # review rows use the usual rolled-back test connection.
        started = clock.now()
        gateway = build_gateway(settings, database, artifacts, clock, transport=transport)
        ctx = AppContext(
            settings=settings,
            db=db,
            artifacts=artifacts,
            clock=clock,
            gateway=gateway,
            ledger=ledger,
            queue=queue,
        )
        # Isolate this worker from any committing tests that ran earlier in the suite.
        pg.execute("update job set run_after = now() + interval '1 day' where status = 'queued'")
        pid, lead = create_project(
            pg, name="Synthetic issue74", lead_name="Synthetic lead", ledger=ledger
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
        report_id = reports(pg, pid, revision.id)[0].report_id
        # Queue the report at the retrieval composition boundary. Screening is independent of
        # request provenance; no internal lookup/persistence function is mocked or called.
        run_id = queue_retrieval(
            pg,
            pid,
            [report_id],
            actor_id=lead,
            trigger="requested",
            ledger=ledger,
            queue=queue,
            clock=clock,
        )
        worker = Worker(db, queue, job_handlers(ctx), name="issue74", lease=timedelta(hours=1))
        with (
            stamina.set_testing(True, attempts=10, cap=True),
            caplog.at_level(logging.INFO, logger="httpx2"),
        ):
            assert worker.run_once()
        gateway.close()

    converter = [r for r in seen if "idconv" in str(r.url)]
    assert converter
    assert all(
        {k.lower(): v for k, v in parse_qsl(urlsplit(str(r.url)).query)}["email"] == CONTACT
        for r in converter
    )
    assert all(dict(parse_qsl(urlsplit(str(r.url)).query))["tool"] == "kittum" for r in converter)
    assert all(CONTACT in r.headers["User-Agent"] for r in seen)
    if answer in ("http_error", "encoded_error", "truncated_error", "escaped_truncated_error"):
        assert len(converter) == 5  # the real gateway's retry path
    if answer == "recovered":
        assert len(converter) == 3
    row = pg.execute("select * from retrieval_run_report where run_id = %s", (run_id,)).fetchone()
    assert row is not None
    if answer in ("pdf", "redirect", "recovered"):
        assert row["outcome"] == "acquired"
        assert any(str(r.url) == PDF_URL for r in seen)
        assert row["attempts"][-1]["url"] == PDF_URL  # useful non-contact provenance
    else:
        assert row["outcome"] == ("no_location" if answer == "none" else "not_acquired")
    assert ledger.verify(pg, pid).ok
    events = ledger.events(pg, pid)
    assert any(e.type == "retrieval.source_asked" for e in events)
    bundle = tmp_path / "bundle"
    signer = SigningKey.generate()
    seal(
        pg,
        pid,
        events[-1].seq,
        artifacts,
        bundle,
        signer=signer,
        actor_id=lead,
        ledger=ledger,
    )
    verification = verify_bundle(bundle, trusted_key=signer.verify_key)
    assert verification.ok, verification
    observations = {
        "ledger": json.dumps([e.model_dump(mode="json") for e in events]),
        "retrieval_run_report": json.dumps(row, default=str),
        "gateway_attempt": json.dumps(
            pg.execute(
                "select * from gateway_attempt where source = 'pmc' and recorded_at >= %s order by recorded_at",
                (started,),
            ).fetchall(),
            default=str,
        ),
        "worker_http_log": caplog.text,
        "worker_retry_log_fields": json.dumps(
            [
                {k: v for k, v in record.__dict__.items() if k.startswith("stamina.")}
                for record in caplog.records
                if record.name == "stamina"
            ],
            default=str,
        ),
        "bundle": "\n".join(
            p.read_bytes().decode("utf-8", errors="ignore")
            for p in bundle.rglob("*")
            if p.is_file()
        ),
    }
    # Redaction must retain the synthetic publication's scholarly contact and useful URLs.
    assert AUTHOR in observations["bundle"]
    leaked = [name for name, text in observations.items() if CONTACT.casefold() in _decoded(text)]
    if answer in ("truncated_error", "escaped_truncated_error"):
        assert all("issue74.operator" not in _decoded(text) for text in observations.values())
    (tmp_path / "observations.json").write_text(
        json.dumps(
            {
                "answer": answer,
                "wire_contact_preserved": True,
                "ledger_verified": True,
                "bundle_verified": True,
                "leaked_surfaces": leaked,
                "observations": observations,
            },
            indent=2,
        )
    )
    assert not leaked, f"configured synthetic operator contact escaped through: {leaked}"
