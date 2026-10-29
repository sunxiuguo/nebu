"""Open-access full texts for the reports sought for retrieval, as a job (DESIGN.md, "Full text").

    run = request_open_access(tx, project_id, actor_id=lead, ...)
    # `kittum worker` runs OPEN_ACCESS_JOB: the waterfall for each report, one at a time
    view = open_access_run(tx, project_id, run.id)

A report is sought for retrieval once it is included at title and abstract in some scope; a run
takes those that have no usable full text, in corpus order, when it is requested. A full text a
reviewer detached again (a wrong document) is not a usable one, so `kittum.app.full_text` decides,
never the history of fetches.

For each report the job walks the waterfall (`kittum.enrich.WATERFALL`). The open-access locations
enrichment found for the report's records come with it (Europe PMC, Unpaywall, OpenAlex, the
publisher's openly licensed PDF links from Crossref). The other sources (PubMed Central, medRxiv
and bioRxiv, arXiv, Semantic Scholar, CORE) are asked about the report when the waterfall reaches
them, each lookup an attempt of its own; a source without a key it needs is left out of the run
and the run says so. Each PDF location is tried in turn: the request goes out with no transaction
open, and each attempt is committed with its ledger event before the next is tried, so a crash or
a failed attach keeps the evidence and a retried job goes on after the last recorded attempt. A
PDF must be the report's own (`kittum.documents.check_report`): a supplement a source named
instead is a wrong document, and the waterfall moves on. A PDF it gets is stored with its licence
and, in a transaction of its own, becomes the report's full text with its parsed layer
(`attach_acquired_full_text`). A report that is uploaded by hand meanwhile is left alone.

`request_retry` mends a source outage without a new run: it enriches again the records a source
failed for, then retrieves the reports among them still without a full text, and retrieves again
the reports a source failed for during retrieval. `missing_full_texts` lists the reports still
without a usable full text with every attempt for them, for the CSV and RIS exports.

Like the upload queue, every run is the methods lead's alone: it names the reports included at
title and abstract.
"""

import csv
import io
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

from psycopg.types.json import Jsonb

from kittum.app.enrichment import (
    ENRICHMENT_JOB,
    enrichment_config,
    lock_project_runs,
    queue_enrichment,
)
from kittum.app.lifecycle import (
    LifecycleError,
    WrongDocumentNote,
    attach_acquired_full_text,
    full_text,
    wrong_documents,
)
from kittum.app.privacy import ContactPrivacy
from kittum.core import Clock, Id, NotFoundError, Record, canonical_doi, new_id
from kittum.corpus import latest_revision, reports
from kittum.documents import (
    AcquisitionAttempt,
    ExpectedReport,
    FullTextLocation,
    fetch_location,
    get_acquisition,
    record_location,
    source_name,
)
from kittum.enrich import (
    LOOKUP_SOURCES,
    RETRYABLE,
    OaLocation,
    RecordEnrichment,
    SourceUnavailable,
    WorkIds,
    full_text_candidates,
    lookup_applies,
    lookup_full_text,
    skipped_source,
    waterfall_rank,
)
from kittum.gateway import default_policies
from kittum.ledger import EventDraft
from kittum.report import ris_dumps, ris_entry
from kittum.runs import JobOutput, JobSpec, PermanentJobError

if TYPE_CHECKING:
    from kittum.app.context import AppContext
    from kittum.db import Tx
    from kittum.enrich import EnrichmentConfig
    from kittum.gateway import Gateway
    from kittum.ledger import Ledger
    from kittum.runs import Handler, Job, JobContext, JobQueue
    from kittum.screening import Screening
    from kittum.search import SourceRecord

OPEN_ACCESS_JOB = "retrieval.open_access"
OPEN_ACCESS_REQUESTED = "retrieval.open_access_requested"
SOURCE_ASKED = "retrieval.source_asked"
NO_LOCATION = "retrieval.no_open_access_location"
OPEN_ACCESS_FINISHED = "retrieval.open_access_finished"
RETRY_REQUESTED = "retrieval.retry_requested"

type ReportOutcome = Literal["acquired", "not_acquired", "no_location", "had_full_text"]
type RunTrigger = Literal["requested", "retry"]
type LookupOutcome = Literal["found", "known", "none", "unavailable", "other_work"]


class AttemptView(Record):
    """One step of the waterfall for one report: a lookup that asked a source about the report,
    or a fetch of one file location."""

    kind: Literal["fetch", "lookup"] = "fetch"
    source: str
    url: str
    host: str
    status: int | None
    outcome: str
    licence: str | None
    error: str | None = None
    message: str | None = None
    """What the source said about its failure, in its own words."""
    check: str | None = None
    """What the check against the record found, for a PDF."""
    found: int | None = None
    """Files a lookup named."""


class SkippedSource(Record):
    source: str
    reason: str


class ReportRetrieval(Record):
    report_id: Id
    title: str
    outcome: ReportOutcome | None
    """None while the report waits its turn."""
    attempts: tuple[AttemptView, ...]
    carried: tuple[AttemptView, ...] = ()
    """Earlier runs' attempts a retry did not repeat, because asking again cannot change them."""
    source: str | None = None
    """Where the full text came from, when one was acquired."""
    licence: str | None = None
    document_id: Id | None = None
    page_count: int | None = None


class OpenAccessRun(Record):
    id: Id
    trigger: RunTrigger = "requested"
    status: Literal["queued", "running", "done", "failed"]
    requested_by: Id
    requested_at: datetime
    finished_at: datetime | None
    reports: tuple[ReportRetrieval, ...]
    skipped: tuple[SkippedSource, ...] = ()
    """Lookup sources the run left out for every report, and why."""
    error: str | None = None
    earlier: tuple[ReportRetrieval, ...] = ()
    """For the newest run only: each report it did not take, as the run that last tried it left
    it, so a retry of a few reports does not hide what earlier runs did for the others."""


def _event(project_id: Id, type: str, actor_id: Id, payload: dict[str, Any]) -> EventDraft:
    return EventDraft(
        project_id=project_id, type=type, schema_version=1, actor_id=actor_id, payload=payload
    )


def sought_for_retrieval(tx: Tx, project_id: Id, screening: Screening) -> list[Id]:
    """The reports of the newest identity revision included at title and abstract in at least one
    scope, in corpus order."""
    revision = latest_revision(tx, project_id)
    if revision is None:
        raise LifecycleError("build the corpus first")
    included: set[str] = set()
    for info in screening.stages(tx, project_id):
        if info.stage != "title_abstract":
            continue
        found = screening.outcomes(
            tx, project_id, info.scope_id, info.stage, topic_id=info.topic_id
        )
        included |= {ref for ref, outcome in found.items() if outcome == "include"}
    return [
        r.report_id for r in reports(tx, project_id, revision.id) if str(r.report_id) in included
    ]


def without_full_text(tx: Tx, project_id: Id, screening: Screening) -> list[Id]:
    """The reports sought for retrieval that have no usable full text now, in corpus order."""
    return [
        r
        for r in sought_for_retrieval(tx, project_id, screening)
        if full_text(tx, project_id, str(r)) is None
    ]


def _active_run(tx: Tx, project_id: Id) -> Id | None:
    row = tx.execute(
        "select r.id from retrieval_run r join job j on j.id = r.job_id"
        " where r.project_id = %s and r.finished_at is null and j.status in ('queued', 'running')"
        " order by r.requested_at desc limit 1",
        (project_id,),
    ).fetchone()
    return row["id"] if row else None


def queue_retrieval(
    tx: Tx,
    project_id: Id,
    report_ids: list[Id],
    *,
    actor_id: Id,
    trigger: RunTrigger,
    ledger: Ledger,
    queue: JobQueue,
    clock: Clock,
) -> Id:
    """Queue a run of the waterfall over `report_ids`, in that order. The caller holds the
    project's retrieval lock and has checked no run is going. A retry carries each report's
    terminal attempts from earlier runs, which its waterfall skips."""
    run_id = new_id()
    carried = _terminal_attempts(tx, project_id, report_ids) if trigger == "retry" else {}
    event = ledger.append(
        tx,
        _event(
            project_id,
            OPEN_ACCESS_REQUESTED,
            actor_id,
            {"run_id": str(run_id), "trigger": trigger, "report_ids": [str(r) for r in report_ids]}
            | ({"carried": {str(r): a for r, a in carried.items()}} if carried else {}),
        ),
    )
    job = queue.enqueue(
        tx,
        JobSpec(
            project_id=project_id,
            kind=OPEN_ACCESS_JOB,
            stage_key=f"{OPEN_ACCESS_JOB}:{run_id}",
            payload={"run_id": str(run_id)},
        ),
    )
    tx.execute(
        "insert into retrieval_run (id, project_id, requested_by, requested_at, job_id, event_id,"
        " trigger) values (%s, %s, %s, %s, %s, %s, %s)",
        (run_id, project_id, actor_id, clock.now(), job.job_id, event.event_id, trigger),
    )
    with tx.cursor() as cur:
        cur.executemany(
            "insert into retrieval_run_report (run_id, report_id, ordinal, carried)"
            " values (%s, %s, %s, %s)",
            [
                (run_id, report_id, i, Jsonb(carried.get(report_id, [])))
                for i, report_id in enumerate(report_ids)
            ],
        )
    return run_id


def _terminal_attempts(
    tx: Tx, project_id: Id, report_ids: list[Id]
) -> dict[Id, list[dict[str, Any]]]:
    """Each report's attempts that asking again cannot change, over every earlier run: a lookup
    whose source answered for another work, a PDF that was another document, and a PDF a reviewer
    detached as the wrong document after it was attached."""
    rows = tx.execute(
        "select rr.report_id, rr.attempts, rr.carried from retrieval_run_report rr"
        " join retrieval_run r on r.id = rr.run_id"
        " where r.project_id = %s and rr.report_id = any(%s) order by r.requested_at, r.id",
        (project_id, report_ids),
    ).fetchall()
    detached = {h for hashes in detached_hashes(tx, project_id).values() for h in hashes}
    out: dict[Id, list[dict[str, Any]]] = {}
    for row in rows:
        for attempt in [*row["carried"], *row["attempts"]]:
            fetch = attempt.get("kind", "fetch") == "fetch"
            terminal = (
                (attempt.get("kind") == "lookup" and attempt["outcome"] == "other_work")
                or (fetch and attempt["outcome"] == "wrong_document")
                # A PDF a reviewer detached as the wrong document since: the same bytes again.
                or (fetch and attempt.get("body_hash") in detached)
            )
            if terminal and attempt not in out.setdefault(row["report_id"], []):
                out[row["report_id"]].append(attempt)
    return {r: a for r, a in out.items() if a}


def detached_hashes(tx: Tx, project_id: Id) -> dict[str, tuple[str, ...]]:
    """The artifact hashes of the PDFs reviewers detached as the wrong document, per record."""
    rows = tx.execute(
        "select f.item_ref, d.artifact_hash from full_text_detachment f"
        " join document d on d.id = f.document_id where f.project_id = %s order by f.recorded_at",
        (project_id,),
    ).fetchall()
    out: dict[str, tuple[str, ...]] = {}
    for row in rows:
        out[row["item_ref"]] = (*out.get(row["item_ref"], ()), row["artifact_hash"])
    return out


def request_open_access(
    tx: Tx,
    project_id: Id,
    screening: Screening,
    *,
    actor_id: Id,
    ledger: Ledger,
    queue: JobQueue,
    clock: Clock,
) -> OpenAccessRun:
    """Queue the waterfall for every report sought for retrieval that has no usable full text. An
    unfinished run is returned as it is; requests for one project take turns, so two at once cannot
    queue two runs."""
    lock_project_runs(tx, project_id, OPEN_ACCESS_JOB)
    active = _active_run(tx, project_id)
    if active is not None:
        return open_access_run(tx, project_id, active)
    retrying = tx.execute(
        "select 1 from enrichment_run r join job j on j.id = r.job_id where r.project_id = %s"
        " and r.retrieve_after is not null and r.finished_at is null"
        " and j.status in ('queued', 'running')",
        (project_id,),
    ).fetchone()
    if retrying is not None:
        # The retry queues its own retrieval when its enrichment is done; a run started now would
        # take its place and leave the locations the retry finds untried.
        raise LifecycleError("A retry is still enriching. Kittum fetches its reports after that.")
    wanted = without_full_text(tx, project_id, screening)
    if not wanted:
        raise LifecycleError("every report sought for retrieval has its full text already")
    run_id = queue_retrieval(
        tx,
        project_id,
        wanted,
        actor_id=actor_id,
        trigger="requested",
        ledger=ledger,
        queue=queue,
        clock=clock,
    )
    return open_access_run(tx, project_id, run_id)


def _attempt(raw: dict[str, Any]) -> AttemptView:
    if raw.get("kind") == "lookup":
        return AttemptView(
            kind="lookup",
            source=raw["source"],
            url=raw["url"],
            host=urlsplit(raw["url"]).hostname or raw["url"],
            status=raw.get("status"),
            outcome=raw["outcome"],
            licence=None,
            error=raw.get("error"),
            message=raw.get("message"),
            found=raw.get("found"),
        )
    attempt = AcquisitionAttempt.model_validate(raw)
    return AttemptView(
        source=attempt.source,
        url=attempt.url,
        host=urlsplit(attempt.url).hostname or attempt.url,
        status=attempt.status,
        outcome=attempt.outcome,
        licence=attempt.licence,
        error=attempt.error,
        check=attempt.check,
        message=attempt.message,
    )


def open_access_run(tx: Tx, project_id: Id, run_id: Id) -> OpenAccessRun:
    row = tx.execute(
        "select r.*, j.status as job_status, j.last_error from retrieval_run r"
        " join job j on j.id = r.job_id where r.project_id = %s and r.id = %s",
        (project_id, run_id),
    ).fetchone()
    if row is None:
        raise NotFoundError(f"retrieval run {run_id} not found")
    facts = report_facts(tx, project_id)
    items = tx.execute(
        "select rr.*, a.source, a.licence, a.document_id, d.page_count"
        " from retrieval_run_report rr"
        " left join acquisition a on a.id = rr.acquisition_id"
        " left join document d on d.id = a.document_id"
        " where rr.run_id = %s order by rr.ordinal",
        (run_id,),
    ).fetchall()
    if row["finished_at"] is not None:
        status, error = "done", None
    elif row["job_status"] in ("failed", "dead"):
        status, error = "failed", row["last_error"]
    else:
        status = "running" if row["job_status"] == "running" else "queued"
        error = row["last_error"]
    return OpenAccessRun(
        id=row["id"],
        trigger=row["trigger"],
        status=status,
        requested_by=row["requested_by"],
        requested_at=row["requested_at"],
        finished_at=row["finished_at"],
        error=error,
        skipped=tuple(SkippedSource.model_validate(s) for s in row["skipped"]),
        reports=tuple(_report(item, facts) for item in items),
    )


def _report(item: dict[str, Any], facts: dict[Id, ReportFacts]) -> ReportRetrieval:
    return ReportRetrieval(
        report_id=item["report_id"],
        title=facts[item["report_id"]].title if item["report_id"] in facts else "",
        outcome=item["outcome"],
        attempts=tuple(_attempt(a) for a in item["attempts"]),
        carried=tuple(_attempt(a) for a in item["carried"]),
        source=item["source"],
        licence=item["licence"],
        document_id=item["document_id"],
        page_count=item["page_count"],
    )


def latest_open_access_run(tx: Tx, project_id: Id) -> OpenAccessRun | None:
    row = tx.execute(
        "select id from retrieval_run where project_id = %s order by requested_at desc, id desc"
        " limit 1",
        (project_id,),
    ).fetchone()
    if row is None:
        return None
    latest = open_access_run(tx, project_id, row["id"])
    taken = [r.report_id for r in latest.reports]
    earlier = tx.execute(
        "select distinct on (rr.report_id) rr.*, a.source, a.licence, a.document_id, d.page_count"
        " from retrieval_run_report rr join retrieval_run r on r.id = rr.run_id"
        " left join acquisition a on a.id = rr.acquisition_id"
        " left join document d on d.id = a.document_id"
        " where r.project_id = %s and rr.outcome is not null and rr.report_id <> all(%s)"
        " order by rr.report_id, r.requested_at desc, r.id desc",
        (project_id, taken),
    ).fetchall()
    facts = report_facts(tx, project_id)
    return latest.model_copy(update={"earlier": tuple(_report(item, facts) for item in earlier)})


def report_attempts(tx: Tx, project_id: Id, report_id: Id) -> list[AttemptView]:
    """Every waterfall attempt for a report, over all runs, oldest first."""
    rows = tx.execute(
        "select rr.attempts from retrieval_run_report rr join retrieval_run r on r.id = rr.run_id"
        " where r.project_id = %s and rr.report_id = %s order by r.requested_at, r.id",
        (project_id, report_id),
    ).fetchall()
    return [_attempt(a) for r in rows for a in r["attempts"]]


@dataclass(frozen=True)
class ReportFacts:
    """What the waterfall and the exports know about one report: its best record, and the
    identifiers of all its records, enrichment's included."""

    record: SourceRecord
    title: str
    doi: str | None
    pmid: str | None
    pmcid: str | None

    @property
    def ids(self) -> WorkIds:
        return WorkIds(doi=self.doi, pmid=self.pmid, pmcid=self.pmcid)

    @property
    def expected(self) -> ExpectedReport:
        return ExpectedReport(doi=self.doi, title=self.title)


def report_facts(tx: Tx, project_id: Id) -> dict[Id, ReportFacts]:
    revision = latest_revision(tx, project_id)
    if revision is None:
        return {}
    enriched = {
        r["occurrence_id"]: r
        for r in tx.execute(
            "select occurrence_id, doi, pmid, pmcid from record_enrichment where project_id = %s",
            (project_id,),
        ).fetchall()
    }
    out: dict[Id, ReportFacts] = {}
    for report in reports(tx, project_id, revision.id):
        best = max(report.records, key=lambda x: len(x.abstract or ""))
        rows = [enriched.get(r.occurrence_id) or {} for r in report.records]

        def first(name: str, rows=rows, report=report) -> str | None:
            own = [getattr(r, name, None) for r in report.records]
            return next((v for v in [*own, *(row.get(name) for row in rows)] if v), None)

        out[report.report_id] = ReportFacts(
            record=best,
            title=best.title,
            doi=canonical_doi(first("doi")),
            pmid=first("pmid"),
            pmcid=first("pmcid"),
        )
    return out


# The job


def _terms() -> dict[str, str]:
    return {source: policy.terms_url for source, policy in default_policies().items()}


def _location(location: OaLocation | Any, terms: dict[str, str]) -> FullTextLocation:
    return FullTextLocation(
        source=location.source,
        url=location.url,
        licence=location.licence,
        terms_url=terms.get(location.source),
    )


def locations(tx: Tx, project_id: Id, report_id: Id, *, on: date) -> list[FullTextLocation]:
    """The PDF locations enrichment found for a report's records, in waterfall order, each with the
    terms of the source it came from; a location whose licence starts after `on` is left out."""
    revision = latest_revision(tx, project_id)
    if revision is None:
        return []
    occurrences = [
        record.occurrence_id
        for report in reports(tx, project_id, revision.id)
        if report.report_id == report_id
        for record in report.records
    ]
    rows = tx.execute(
        "select oa_locations from record_enrichment where project_id = %s"
        " and occurrence_id = any(%s)",
        (project_id, occurrences),
    ).fetchall()
    merged = RecordEnrichment(
        occurrence_id=report_id,
        provider_record_id=str(report_id),
        oa_locations=tuple(OaLocation.model_validate(o) for r in rows for o in r["oa_locations"]),
    )
    terms = _terms()
    seen: set[str] = set()
    found = []
    for candidate in full_text_candidates(merged, on=on):
        if candidate.format != "pdf" or candidate.url in seen:
            continue
        seen.add(candidate.url)
        found.append(_location(candidate, terms))
    return found


type _Step = tuple[Literal["fetch"], FullTextLocation] | tuple[Literal["lookup"], str]


def next_step(
    row_locations: list[dict[str, Any]],
    attempts: list[dict[str, Any]],
    *,
    ids: WorkIds,
    config: EnrichmentConfig,
) -> _Step | None:
    """The waterfall's next step for a report: a location not tried yet, or a lookup source not
    asked yet, whichever comes first in the waterfall; `None` when nothing is left."""
    tried = {a["url"] for a in attempts if a.get("kind", "fetch") == "fetch"}
    asked = {a["source"] for a in attempts if a.get("kind") == "lookup"}
    pending = [
        (waterfall_rank(loc["source"]), i, loc)
        for i, loc in enumerate(row_locations)
        if loc["url"] not in tried
    ]
    lookup = next(
        (s for s in LOOKUP_SOURCES if s not in asked and lookup_applies(s, ids, config=config)),
        None,
    )
    fetch = min(pending, key=lambda p: (p[0], p[1])) if pending else None
    if fetch is not None and (lookup is None or fetch[0] < waterfall_rank(lookup)):
        return ("fetch", FullTextLocation.model_validate(fetch[2]))
    if lookup is not None:
        return ("lookup", lookup)
    return None


class _Report:
    """One report's waterfall, a step per transaction. Every step takes the job's lease (`fence`)
    and the report's row lock, and re-reads the row, so a worker that lost its lease writes
    nothing and two workers never write over each other's progress."""

    def __init__(
        self,
        ctx: AppContext,
        gateway: Gateway,
        ledger: Ledger,
        queue: JobQueue,
        job: Job,
        run: dict[str, Any],
        report_id: Id,
        config: EnrichmentConfig,
    ) -> None:
        self.ctx, self.gateway, self.ledger, self.queue, self.job = ctx, gateway, ledger, queue, job
        self.run, self.report_id, self.config = run, report_id, config
        self.project_id, self.actor = run["project_id"], run["requested_by"]
        self.ref = str(report_id)
        self.facts: ReportFacts | None = None
        self.refused: tuple[str, ...] = ()

    def _row(self, tx: Tx) -> dict[str, Any]:
        self.queue.fence(tx, self.job)
        row = tx.execute(
            "select * from retrieval_run_report where run_id = %s and report_id = %s for update",
            (self.run["id"], self.report_id),
        ).fetchone()
        assert row is not None
        return row

    def _finish(self, tx: Tx, outcome: ReportOutcome, event_id: Id | None = None) -> None:
        tx.execute(
            "update retrieval_run_report set outcome = %s, event_id = coalesce(%s, event_id),"
            " finished_at = %s where run_id = %s and report_id = %s",
            (outcome, event_id, self.ctx.clock.now(), self.run["id"], self.report_id),
        )

    @property
    def ids(self) -> WorkIds:
        return self.facts.ids if self.facts else WorkIds()

    def run_waterfall(self) -> None:
        with self.ctx.db.transaction() as tx:
            row = self._row(tx)
            if row["outcome"] is not None:
                return
            self.facts = report_facts(tx, self.project_id).get(self.report_id)
            self.refused = detached_hashes(tx, self.project_id).get(self.ref, ())
            if row["locations"] is None:
                if full_text(tx, self.project_id, self.ref) is not None:
                    self._finish(tx, "had_full_text")  # uploaded by hand since the request
                    return
                found = locations(
                    tx, self.project_id, self.report_id, on=self.ctx.clock.now().date()
                )
                tx.execute(
                    "update retrieval_run_report set locations = %s"
                    " where run_id = %s and report_id = %s",
                    (
                        Jsonb([f.model_dump(mode="json") for f in found]),
                        self.run["id"],
                        self.report_id,
                    ),
                )
        while self._step():
            pass
        self._attach()

    def _step(self) -> bool:
        """Take the next step; False once a PDF was acquired or nothing is left."""
        with self.ctx.db.transaction() as tx:
            row = self._row(tx)
            done = len(row["attempts"])
            if row["outcome"] is not None or row["acquisition_id"] is not None:
                return False
            step = next_step(
                row["locations"],
                [*row["carried"], *row["attempts"]],
                ids=self.ids,
                config=self.config,
            )
        if step is None:
            return False
        if step[0] == "lookup":
            return self._lookup(step[1], done)
        return self._fetch(step[1], done)

    def _lookup(self, source: str, done: int) -> bool:
        error: SourceUnavailable | None = None
        try:  # outside any transaction
            found = lookup_full_text(self.gateway, source, self.ids, config=self.config)
        except SourceUnavailable as e:
            found, error = None, e
        terms = _terms()
        new = [_location(loc, terms) for loc in (found.locations if found else ())]
        with self.ctx.db.transaction() as tx:
            row = self._row(tx)
            if row["outcome"] is not None or len(row["attempts"]) != done:
                return True  # another worker moved on meanwhile; read its progress again
            known = {loc["url"] for loc in row["locations"]}
            added = [loc.model_dump(mode="json") for loc in new if loc.url not in known]
            outcome: LookupOutcome
            if error is not None:
                outcome = "other_work" if error.kind == "other_work" else "unavailable"
            else:
                outcome = "found" if added else "known" if new else "none"
            # Lookup URLs are audit data. A retry asks this source again with a request rebuilt
            # from config; it never sends this redacted URL or expands its placeholder.
            privacy = ContactPrivacy(self.config.contact_email)
            attempt: dict[str, Any] = privacy.value(
                {
                    "kind": "lookup",
                    "source": source,
                    "url": (found.url if found else error.url if error else None) or "",
                    "status": error.status if error else None,
                    "outcome": outcome,
                    "error": error.reason if error else None,
                    "message": error.message if error else None,
                    "found": len(added),
                }
            )
            self.ledger.append(
                tx,
                _event(
                    self.project_id,
                    SOURCE_ASKED,
                    self.actor,
                    {"run_id": str(self.run["id"]), "report_id": self.ref, "attempt": attempt}
                    | {"locations": added},
                ),
            )
            tx.execute(
                "update retrieval_run_report set attempts = attempts || %s,"
                " locations = locations || %s where run_id = %s and report_id = %s",
                (Jsonb([attempt]), Jsonb(added), self.run["id"], self.report_id),
            )
        return True

    def _fetch(self, location: FullTextLocation, done: int) -> bool:
        fetched = fetch_location(self.gateway, location)  # outside any transaction
        with self.ctx.db.transaction() as tx:
            row = self._row(tx)
            if row["outcome"] is not None or len(row["attempts"]) != done:
                return True  # another worker moved on meanwhile; read its progress again
            result = record_location(
                tx,
                fetched,
                artifacts=self.ctx.artifacts,
                ledger=self.ledger,
                clock=self.ctx.clock,
                project_id=self.project_id,
                actor_id=self.actor,
                report_id=self.ref,
                expected=(
                    self.facts.expected.model_copy(update={"refused": self.refused})
                    if self.facts
                    else None
                ),
            )
            tx.execute(
                "update retrieval_run_report set attempts = attempts || %s, acquisition_id = %s"
                " where run_id = %s and report_id = %s",
                (
                    Jsonb([a.model_dump(mode="json") for a in result.attempts]),
                    result.acquisition.id if result.acquisition else None,
                    self.run["id"],
                    self.report_id,
                ),
            )
            return result.acquisition is None

    def _attach(self) -> None:
        with self.ctx.db.transaction() as tx:
            row = self._row(tx)
            if row["outcome"] is not None:
                return
            if row["acquisition_id"] is None:
                # A location tried, or a source that failed, is more than "no location known",
                # in this run or in an earlier one whose attempt a retry carries.
                tried = any(
                    a.get("kind", "fetch") == "fetch"
                    or a["outcome"] in ("unavailable", "other_work")
                    for a in [*row["carried"], *row["attempts"]]
                )
                if tried:
                    self._finish(tx, "not_acquired")
                    return
                event = self.ledger.append(
                    tx,
                    _event(
                        self.project_id,
                        NO_LOCATION,
                        self.actor,
                        {"run_id": str(self.run["id"]), "item_ref": self.ref},
                    ),
                )
                self._finish(tx, "no_location", event.event_id)
                return
            acquisition = get_acquisition(tx, row["acquisition_id"])
            if full_text(tx, self.project_id, self.ref) is not None:
                # Uploaded by hand after the PDF was fetched: the upload stays the report's full
                # text, and the acquisition stays on record.
                self._finish(tx, "had_full_text")
                return
            attach_acquired_full_text(
                tx,
                self.project_id,
                self.ref,
                acquisition,
                actor_id=self.actor,
                artifacts=self.ctx.artifacts,
                ledger=self.ledger,
            )
            self._finish(tx, "acquired")


def open_access_handler(
    ctx: AppContext, gateway: Gateway, ledger: Ledger, queue: JobQueue
) -> Handler:
    config = enrichment_config(ctx)
    skipped = [
        {"source": s, "reason": reason}
        for s in LOOKUP_SOURCES
        if (reason := skipped_source(s, config)) is not None
    ]

    def handle(job: Job, job_ctx: JobContext) -> JobOutput:
        run_id = UUID(job.spec.payload["run_id"])
        with ctx.db.transaction() as tx:
            queue.fence(tx, job)
            run = tx.execute(
                "update retrieval_run set skipped = %s where id = %s returning *",
                (Jsonb(skipped), run_id),
            ).fetchone()
            if run is None:
                raise PermanentJobError(f"retrieval run {run_id} not found")
            pending = [
                r["report_id"]
                for r in tx.execute(
                    "select report_id from retrieval_run_report where run_id = %s"
                    " and outcome is null order by ordinal",
                    (run_id,),
                ).fetchall()
            ]
        for report_id in pending:
            if job_ctx.lease_lost.is_set():
                return JobOutput()  # the heartbeat saw another worker take over
            _Report(ctx, gateway, ledger, queue, job, run, report_id, config).run_waterfall()

        def publish(tx: Tx) -> None:
            view = open_access_run(tx, run["project_id"], run_id)
            counts: dict[str, int] = {}
            for item in view.reports:
                counts[item.outcome or "pending"] = counts.get(item.outcome or "pending", 0) + 1
            ledger.append(
                tx,
                _event(
                    run["project_id"],
                    OPEN_ACCESS_FINISHED,
                    run["requested_by"],
                    {"run_id": str(run_id), "outcomes": counts, "skipped": skipped},
                ),
            )
            tx.execute(
                "update retrieval_run set finished_at = %s where id = %s",
                (ctx.clock.now(), run_id),
            )

        return JobOutput(publish=publish)

    return handle


# Retrying failed sources


class FailedSource(Record):
    """A source that failed where asking again can help, with the last failure it gave."""

    source: str
    records: int
    """Records it failed for in their last enrichment."""
    reports: int
    """Reports without a full text it failed for in their last retrieval."""
    status: int | None = None
    message: str | None = None
    reason: str


class RetryPreview(Record):
    """What a retry would ask again: records to enrich and reports to retrieve, by source."""

    occurrences: int
    reports: int
    sources: tuple[FailedSource, ...]


class RetryPlan(Record):
    """What `request_retry` queued: the records enriched again, and the reports retrieved again
    (after that enrichment, when there is one)."""

    occurrences: int
    reports: int
    sources: tuple[str, ...]
    """The sources that failed, in waterfall order."""
    enrichment_run_id: Id | None = None
    retrieval_run_id: Id | None = None


def _retryable(attempt: dict[str, Any]) -> bool:
    """An attempt that failed because its source did, not because of the work: no answer, a
    server error, or too many requests."""
    if attempt.get("kind") == "lookup":
        return attempt["outcome"] == "unavailable"
    status = attempt.get("status")
    return attempt["outcome"] == "unavailable" or (
        attempt["outcome"] == "http_error"
        and status is not None
        and (status >= 500 or status == 429)
    )


@dataclass
class _Targets:
    revision_id: Id
    occurrences: dict[Id, list[dict[str, Any]]]
    """Records to enrich again, each with its retryable failures."""
    report_ids: list[Id]
    """Reports to retrieve again, in corpus order."""
    attempts: dict[Id, list[dict[str, Any]]]
    """The retryable attempts of the reports a source failed for in retrieval."""


def _targets(tx: Tx, project_id: Id, screening: Screening) -> _Targets:
    revision = latest_revision(tx, project_id)
    if revision is None:
        raise LifecycleError("build the corpus first")
    in_revision = {
        record.occurrence_id: report.report_id
        for report in reports(tx, project_id, revision.id)
        for record in report.records
    }
    occurrences: dict[Id, list[dict[str, Any]]] = {}
    for row in tx.execute(
        "select distinct on (e.occurrence_id) e.occurrence_id, e.failed"
        " from enrichment_run_record e join enrichment_run r on r.id = e.run_id"
        " where r.project_id = %s order by e.occurrence_id, r.requested_at desc, r.id desc",
        (project_id,),
    ).fetchall():
        failed = [f for f in row["failed"] if f.get("kind", "http_error") in RETRYABLE]
        if failed and row["occurrence_id"] in in_revision:
            occurrences[row["occurrence_id"]] = failed
    attempts: dict[Id, list[dict[str, Any]]] = {}
    for row in tx.execute(
        "select distinct on (rr.report_id) rr.report_id, rr.attempts"
        " from retrieval_run_report rr join retrieval_run r on r.id = rr.run_id"
        " where r.project_id = %s and rr.outcome is not null"
        " order by rr.report_id, r.requested_at desc, r.id desc",
        (project_id,),
    ).fetchall():
        failed = [a for a in row["attempts"] if _retryable(a)]
        if failed:
            attempts[row["report_id"]] = failed
    affected = {in_revision[o] for o in occurrences} | set(attempts)
    report_ids = [r for r in without_full_text(tx, project_id, screening) if r in affected]
    return _Targets(
        revision_id=revision.id,
        occurrences=occurrences,
        report_ids=report_ids,
        attempts={r: attempts[r] for r in report_ids if r in attempts},
    )


def retry_preview(tx: Tx, project_id: Id, screening: Screening) -> RetryPreview:
    """What `request_retry` would ask again now, by failed source in waterfall order."""
    targets = _targets(tx, project_id, screening)
    records: dict[str, int] = {}
    reports_by: dict[str, int] = {}
    last: dict[str, dict[str, Any]] = {}
    for failures in targets.occurrences.values():
        for f in failures:
            records[f["source"]] = records.get(f["source"], 0) + 1
            last[f["source"]] = f
    for failed in targets.attempts.values():
        for source in dict.fromkeys(a["source"] for a in failed):
            reports_by[source] = reports_by.get(source, 0) + 1
        for a in failed:
            last.setdefault(a["source"], {"reason": a.get("error") or a["outcome"]} | a)
    return RetryPreview(
        occurrences=len(targets.occurrences),
        reports=len(targets.report_ids),
        sources=tuple(
            FailedSource(
                source=source,
                records=records.get(source, 0),
                reports=reports_by.get(source, 0),
                status=last[source].get("status"),
                message=last[source].get("message"),
                reason=last[source].get("reason") or last[source].get("error") or "",
            )
            for source in sorted(last, key=waterfall_rank)
        ),
    )


def request_retry(
    tx: Tx,
    project_id: Id,
    screening: Screening,
    *,
    actor_id: Id,
    ledger: Ledger,
    queue: JobQueue,
    clock: Clock,
) -> RetryPlan:
    """Ask the failed sources again. The records a source failed for in their last enrichment are
    enriched again; once that is done, the reports among them sought for retrieval and still
    without a full text are retrieved again, with the reports a source failed for in their last
    retrieval. Refused while an enrichment or a retrieval is still going, or when no source
    failed."""
    lock_project_runs(tx, project_id, ENRICHMENT_JOB)
    lock_project_runs(tx, project_id, OPEN_ACCESS_JOB)
    busy = tx.execute(
        "select 1 from enrichment_run r join job j on j.id = r.job_id where r.project_id = %s"
        " and r.finished_at is null and j.status in ('queued', 'running')",
        (project_id,),
    ).fetchone()
    if busy is not None or _active_run(tx, project_id) is not None:
        raise LifecycleError("A run is still going. Retry once it is done.")
    preview = retry_preview(tx, project_id, screening)
    targets = _targets(tx, project_id, screening)
    if not targets.occurrences and not targets.report_ids:
        raise LifecycleError("No source failed, so there is nothing to retry.")
    sources = tuple(s.source for s in preview.sources)
    occurrence_ids = sorted(targets.occurrences)
    ledger.append(
        tx,
        _event(
            project_id,
            RETRY_REQUESTED,
            actor_id,
            {
                "occurrence_ids": [str(o) for o in occurrence_ids],
                "report_ids": [str(r) for r in targets.report_ids],
                "sources": [s.model_dump(mode="json") for s in preview.sources],
            },
        ),
    )
    if occurrence_ids:
        run = queue_enrichment(
            tx,
            project_id,
            targets.revision_id,
            actor_id=actor_id,
            trigger="retry",
            occurrence_ids=occurrence_ids,
            retrieve_after=targets.report_ids,
            ledger=ledger,
            queue=queue,
            clock=clock,
        )
        return RetryPlan(
            occurrences=len(occurrence_ids),
            reports=len(targets.report_ids),
            sources=sources,
            enrichment_run_id=run,
        )
    run_id = queue_retrieval(
        tx,
        project_id,
        targets.report_ids,
        actor_id=actor_id,
        trigger="retry",
        ledger=ledger,
        queue=queue,
        clock=clock,
    )
    return RetryPlan(
        occurrences=0, reports=len(targets.report_ids), sources=sources, retrieval_run_id=run_id
    )


# The reports still without a full text


class MissingReport(Record):
    """A report sought for retrieval without a usable full text, for the exports."""

    report_id: Id
    title: str
    doi: str | None
    pmid: str | None
    authors: tuple[str, ...]
    journal: str | None
    year: int | None
    file_name: str
    """What to name its PDF so that a bulk upload matches it by name."""
    attempts: tuple[AttemptView, ...]
    wrong_document: WrongDocumentNote | None = None
    """A reviewer's "wrong document" on the PDF it had before."""


def suggested_file_name(doi: str | None, pmid: str | None, report_id: Id) -> str:
    if doi:
        return f"{doi.replace('/', '_')}.pdf"
    if pmid:
        return f"PMID_{pmid}.pdf"
    return f"kittum_{report_id}.pdf"


def missing_full_texts(tx: Tx, project_id: Id, screening: Screening) -> list[MissingReport]:
    """Every report sought for retrieval that has no usable full text, in corpus order, with every
    attempt any run made for it."""
    facts = report_facts(tx, project_id)
    wrong = wrong_documents(tx, project_id)
    out = []
    for report_id in without_full_text(tx, project_id, screening):
        found = facts[report_id]
        out.append(
            MissingReport(
                report_id=report_id,
                title=found.title,
                doi=found.doi,
                pmid=found.pmid,
                authors=found.record.authors,
                journal=found.record.journal,
                year=found.record.year,
                file_name=suggested_file_name(found.doi, found.pmid, report_id),
                attempts=tuple(report_attempts(tx, project_id, report_id)),
                wrong_document=wrong.get(str(report_id)),
            )
        )
    return out


_PROBLEM = {
    503: "the service was down",
    502: "a server in front of it failed",
    504: "a server in front of it timed out",
    500: "it had an internal error",
    429: "it was asked too often",
}


def attempt_line(attempt: AttemptView) -> str:
    """One attempt in plain words, as the exports and the ledger readers show it."""
    name = source_name(attempt.source)
    if attempt.kind == "lookup":
        match attempt.outcome:
            case "found":
                files = "file" if attempt.found == 1 else "files"
                return f"{name} named {attempt.found} {files}."
            case "none":
                return f"{name} has no open copy."
            case "known":
                return f"{name} named a file an earlier source named."
            case "other_work":
                return f"{name} answered for another work."
    where = f"{name} ({attempt.host})" if attempt.kind == "fetch" else name
    match attempt.outcome:
        case "acquired":
            return f"{where} gave the PDF."
        case "http_error" if not (
            attempt.status and (attempt.status >= 500 or attempt.status == 429)
        ):
            said = f' It said: "{attempt.message}"' if attempt.message else ""
            return f"{where} refused the download (HTTP {attempt.status}).{said}"
        case "not_pdf":
            return f"{where} gave a web page, not a PDF."
        case "unreadable_pdf":
            return f"{where} gave a PDF that does not open."
        case "wrong_document":
            return f"{where} gave a different document. {attempt.check or ''}".strip()
    problem = _PROBLEM.get(attempt.status or 0)
    detail = (
        f" (HTTP {attempt.status}, {problem})"
        if problem
        else (f" (HTTP {attempt.status})" if attempt.status else "")
    )
    said = f' It said: "{attempt.message}"' if attempt.message else ""
    return f"{name} did not answer{detail}.{said}"


def _why(missing: MissingReport) -> list[str]:
    """Each attempt in plain words, after a reviewer's "wrong document" on the earlier PDF."""
    lines = [attempt_line(a) for a in missing.attempts]
    if missing.wrong_document is not None:
        said = missing.wrong_document
        note = f' Their note: "{said.note}"' if said.note else ""
        lines.insert(0, f"{said.reviewer} said the earlier PDF was the wrong document.{note}")
    return lines


def _tried(attempts: tuple[AttemptView, ...]) -> list[str]:
    return list(dict.fromkeys(source_name(a.source) for a in attempts))


def missing_csv(missing: list[MissingReport]) -> bytes:
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(
        [
            "DOI",
            "PMID",
            "Title",
            "Authors",
            "Journal",
            "Year",
            "Sources tried",
            "Why each attempt failed",
            "Name the PDF",
        ]
    )
    for m in missing:
        writer.writerow(
            [
                m.doi or "",
                m.pmid or "",
                m.title,
                "; ".join(m.authors),
                m.journal or "",
                m.year or "",
                "; ".join(_tried(m.attempts)) or "None yet",
                " ".join(_why(m)) or "No source was asked yet.",
                m.file_name,
            ]
        )
    return out.getvalue().encode()


def missing_ris(tx: Tx, project_id: Id, missing: list[MissingReport]) -> bytes:
    facts = report_facts(tx, project_id)
    entries = []
    for m in missing:
        notes = [
            "Kittum has no usable full text for this report.",
            f"Sources tried: {'; '.join(_tried(m.attempts)) or 'none yet'}.",
            *_why(m),
            f"Name the PDF {m.file_name}",
        ]
        entry = ris_entry(facts[m.report_id].record, report_id=m.report_id, notes=notes)
        if m.doi and "doi" not in entry:
            entry["doi"] = m.doi
        entries.append(entry)
    return ris_dumps(entries)


__all__ = [
    "NO_LOCATION",
    "OPEN_ACCESS_FINISHED",
    "OPEN_ACCESS_JOB",
    "OPEN_ACCESS_REQUESTED",
    "RETRY_REQUESTED",
    "SOURCE_ASKED",
    "AttemptView",
    "FailedSource",
    "MissingReport",
    "OpenAccessRun",
    "ReportFacts",
    "ReportRetrieval",
    "RetryPlan",
    "RetryPreview",
    "SkippedSource",
    "attempt_line",
    "latest_open_access_run",
    "locations",
    "missing_csv",
    "missing_full_texts",
    "missing_ris",
    "next_step",
    "open_access_handler",
    "open_access_run",
    "queue_retrieval",
    "report_attempts",
    "report_facts",
    "request_open_access",
    "request_retry",
    "retry_preview",
    "sought_for_retrieval",
    "suggested_file_name",
    "without_full_text",
]
