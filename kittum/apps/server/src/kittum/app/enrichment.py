"""Enrichment of a corpus revision, as a job (DESIGN.md, "Enrichment and federation").

    run = request_enrichment(tx, ctx, project_id, actor_id=lead, trigger="requested")
    # `kittum worker` runs ENRICHMENT_JOB: chunks of records through `kittum.enrich.enrich`
    view = enrichment_run(tx, project_id, run.id)

The corpus is enriched once it is built, and again whenever the lead asks. A retry
(`kittum.app.open_access.request_retry`) enriches again only the records a source failed for, and
then retrieves the reports among them that still lack a full text. A run fixes the identity
revision it reads. Its job enriches the revision's source occurrences in chunks, each in
a transaction of its own with the chunk's `record_enrichment` rows, a row per record, and a ledger
event naming the source of every field; a retried job skips the records a run has done. A source
that fails is recorded against the records it did not answer, and the run goes on with the next
healthy source ("first healthy source"). Nothing here is shown to anyone but the methods lead: it
lists every record of the catalogue.
"""

from collections import Counter
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

from psycopg.types.json import Jsonb

from kittum.app.privacy import ContactPrivacy
from kittum.core import Clock, Id, NotFoundError, Record, new_id
from kittum.corpus import latest_revision, propose_study_links, reports
from kittum.enrich import (
    SOURCES,
    Enrichment,
    EnrichmentConfig,
    EntityTag,
    FailureKind,
    FieldSource,
    OaLocation,
    SourceFailure,
    WorkUpdate,
    lookup,
    save_enrichment,
)
from kittum.ledger import EventDraft
from kittum.runs import JobOutput, JobSpec, PermanentJobError

if TYPE_CHECKING:
    from kittum.app.context import AppContext
    from kittum.db import Tx
    from kittum.gateway import Gateway
    from kittum.ledger import Ledger
    from kittum.runs import Handler, Job, JobContext, JobQueue
    from kittum.search import SourceRecord

ENRICHMENT_JOB = "enrich.revision"
ENRICHMENT_REQUESTED = "enrichment.requested"
RECORDS_ENRICHED = "enrichment.records_enriched"
ENRICHMENT_FINISHED = "enrichment.finished"

type RunStatus = Literal["queued", "running", "done", "failed"]
type Trigger = Literal["corpus_built", "requested", "retry"]


class SourceFailureView(Record):
    source: str
    reason: str
    kind: FailureKind = "http_error"
    status: int | None = None
    """The last HTTP status, when the source answered at all."""
    message: str | None = None
    """What the source said about the failure, in its own words."""


class SourceProgress(Record):
    """One source's part in a run: records it was asked about, records it filled a field of, and
    records it failed for without answering, with the last failure."""

    source: str
    asked: int
    answered: int
    failed: int
    last_error: str | None = None
    last_failure: SourceFailureView | None = None


class EnrichmentRun(Record):
    id: Id
    identity_revision_id: Id
    trigger: Trigger
    status: RunStatus
    requested_by: Id
    requested_at: datetime
    finished_at: datetime | None
    records: int
    """Source occurrences in the revision."""
    done: int
    retracted: int
    """Records a retraction notice was found for."""
    sources: tuple[SourceProgress, ...]
    error: str | None = None
    """Why the job stopped, when it did."""


class RecordEnrichmentView(Record):
    """One source occurrence as enrichment completed it. `entities` index into
    `title + " " + abstract`, the text the tags were anchored in."""

    occurrence_id: Id
    search_source: str
    """The search that found the record."""
    title: str
    abstract: str | None
    doi: str | None
    pmid: str | None
    pmcid: str | None
    retracted: bool
    retraction_notice_doi: str | None
    updates: tuple[WorkUpdate, ...]
    clinical_trial_numbers: tuple[str, ...]
    oa_locations: tuple[OaLocation, ...]
    entities: tuple[EntityTag, ...]
    field_sources: tuple[FieldSource, ...]
    failures: tuple[SourceFailureView, ...]
    """Sources that failed for this record in the run that last enriched it."""
    enriched_at: datetime


class RecordFlag(Record):
    """What the corpus list shows beside a record."""

    occurrence_id: Id
    retracted: bool
    updates: tuple[str, ...]
    """Kinds of notice that update the record (`correction`, ...), retractions aside."""
    sources: tuple[str, ...]
    open_access: bool


def _event(project_id: Id, type: str, actor_id: Id, payload: dict[str, Any]) -> EventDraft:
    return EventDraft(
        project_id=project_id, type=type, schema_version=1, actor_id=actor_id, payload=payload
    )


def lock_project_runs(tx: Tx, project_id: Id, kind: str) -> None:
    """Serialize the requests of one kind of run for one project until `tx` ends."""
    tx.execute("select pg_advisory_xact_lock(hashtext(%s), hashtext(%s::text))", (kind, project_id))


def _occurrences(tx: Tx, project_id: Id, revision_id: Id) -> list[SourceRecord]:
    """The revision's source occurrences in the order they were imported (their IDs are UUIDv7),
    so the batches a run sends, and so its requests, are the same however the corpus is listed."""
    seen: dict[Id, SourceRecord] = {}
    for report in reports(tx, project_id, revision_id):
        for record in report.records:
            seen.setdefault(record.occurrence_id, record)
    return sorted(seen.values(), key=lambda r: r.occurrence_id)


def request_enrichment(
    tx: Tx,
    project_id: Id,
    *,
    actor_id: Id,
    trigger: Trigger,
    ledger: Ledger,
    queue: JobQueue,
    clock: Clock,
) -> EnrichmentRun:
    """Queue an enrichment of the newest identity revision; an unfinished run is returned as it
    is instead of queueing a second one. Requests for one project take turns (a transaction lock),
    so two at once cannot both find no run and queue two."""
    lock_project_runs(tx, project_id, ENRICHMENT_JOB)
    revision = latest_revision(tx, project_id)
    if revision is None:
        raise NotFoundError("the corpus is not built yet")
    active = tx.execute(
        "select r.id from enrichment_run r join job j on j.id = r.job_id"
        " where r.project_id = %s and r.finished_at is null and j.status in ('queued', 'running')"
        " order by r.requested_at desc limit 1",
        (project_id,),
    ).fetchone()
    if active is not None:
        return enrichment_run(tx, project_id, active["id"])
    run_id = queue_enrichment(
        tx,
        project_id,
        revision.id,
        actor_id=actor_id,
        trigger=trigger,
        ledger=ledger,
        queue=queue,
        clock=clock,
    )
    return enrichment_run(tx, project_id, run_id)


def queue_enrichment(
    tx: Tx,
    project_id: Id,
    revision_id: Id,
    *,
    actor_id: Id,
    trigger: Trigger,
    ledger: Ledger,
    queue: JobQueue,
    clock: Clock,
    occurrence_ids: list[Id] | None = None,
    retrieve_after: list[Id] | None = None,
) -> Id:
    """Queue a run over the revision's records, or only `occurrence_ids`. With `retrieve_after`,
    the run queues open-access retrieval of those reports once it is done. The caller holds the
    project's enrichment lock and has checked no run is going."""
    run_id = new_id()
    records = (
        len(occurrence_ids)
        if occurrence_ids is not None
        else len(_occurrences(tx, project_id, revision_id))
    )
    event = ledger.append(
        tx,
        _event(
            project_id,
            ENRICHMENT_REQUESTED,
            actor_id,
            {
                "run_id": str(run_id),
                "identity_revision_id": str(revision_id),
                "trigger": trigger,
                "records": records,
            }
            | (
                {"occurrence_ids": [str(o) for o in occurrence_ids]}
                if occurrence_ids is not None
                else {}
            )
            | (
                {"retrieve_after": [str(r) for r in retrieve_after]}
                if retrieve_after is not None
                else {}
            ),
        ),
    )
    job = queue.enqueue(
        tx,
        JobSpec(
            project_id=project_id,
            kind=ENRICHMENT_JOB,
            stage_key=f"{ENRICHMENT_JOB}:{run_id}",
            payload={"run_id": str(run_id)},
        ),
    )
    tx.execute(
        "insert into enrichment_run (id, project_id, identity_revision_id, trigger, requested_by,"
        " requested_at, records, job_id, event_id, occurrence_ids, retrieve_after)"
        " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            run_id,
            project_id,
            revision_id,
            trigger,
            actor_id,
            clock.now(),
            records,
            job.job_id,
            event.event_id,
            Jsonb([str(o) for o in occurrence_ids]) if occurrence_ids is not None else None,
            Jsonb([str(r) for r in retrieve_after]) if retrieve_after is not None else None,
        ),
    )
    return run_id


def _status(row: dict[str, Any]) -> tuple[RunStatus, str | None]:
    if row["finished_at"] is not None:
        return "done", None
    if row["job_status"] in ("failed", "dead"):
        return "failed", row["last_error"]
    return ("running" if row["job_status"] == "running" else "queued"), row["last_error"]


def enrichment_run(tx: Tx, project_id: Id, run_id: Id) -> EnrichmentRun:
    row = tx.execute(
        "select r.*, j.status as job_status, j.last_error from enrichment_run r"
        " join job j on j.id = r.job_id where r.project_id = %s and r.id = %s",
        (project_id, run_id),
    ).fetchone()
    if row is None:
        raise NotFoundError(f"enrichment run {run_id} not found")
    done = tx.execute(
        "select asked, answered, failed, retracted from enrichment_run_record where run_id = %s",
        (run_id,),
    ).fetchall()
    asked: Counter[str] = Counter()
    answered: Counter[str] = Counter()
    failed: Counter[str] = Counter()
    last: dict[str, SourceFailureView] = {}
    for record in done:
        asked.update(record["asked"])
        answered.update(record["answered"])
        for failure in record["failed"]:
            failed[failure["source"]] += 1
            last[failure["source"]] = SourceFailureView.model_validate(failure)
    status, error = _status(row)
    return EnrichmentRun(
        id=row["id"],
        identity_revision_id=row["identity_revision_id"],
        trigger=row["trigger"],
        status=status,
        requested_by=row["requested_by"],
        requested_at=row["requested_at"],
        finished_at=row["finished_at"],
        records=row["records"],
        done=len(done),
        retracted=sum(1 for record in done if record["retracted"]),
        sources=tuple(
            SourceProgress(
                source=source,
                asked=asked[source],
                answered=answered[source],
                failed=failed[source],
                last_error=last[source].reason if source in last else None,
                last_failure=last.get(source),
            )
            for source in SOURCES
        ),
        error=error,
    )


def latest_enrichment_run(tx: Tx, project_id: Id) -> EnrichmentRun | None:
    row = tx.execute(
        "select id from enrichment_run where project_id = %s order by requested_at desc, id desc"
        " limit 1",
        (project_id,),
    ).fetchone()
    return enrichment_run(tx, project_id, row["id"]) if row else None


def record_enrichment(tx: Tx, project_id: Id, occurrence_id: Id) -> RecordEnrichmentView:
    """A record of the newest identity revision as enrichment last completed it."""
    row = tx.execute(
        "select * from record_enrichment where project_id = %s and occurrence_id = %s",
        (project_id, occurrence_id),
    ).fetchone()
    revision = latest_revision(tx, project_id)
    found = (
        next(
            (
                r
                for r in _occurrences(tx, project_id, revision.id)
                if r.occurrence_id == occurrence_id
            ),
            None,
        )
        if revision
        else None
    )
    if row is None or found is None:
        raise NotFoundError(f"record {occurrence_id} is not enriched")
    failures = tx.execute(
        "select e.failed from enrichment_run_record e join enrichment_run r on r.id = e.run_id"
        " where r.project_id = %s and e.occurrence_id = %s order by r.requested_at desc limit 1",
        (project_id, occurrence_id),
    ).fetchone()
    return RecordEnrichmentView(
        occurrence_id=occurrence_id,
        search_source=found.source,
        title=found.title,
        abstract=row["abstract"],
        doi=row["doi"],
        pmid=row["pmid"],
        pmcid=row["pmcid"],
        retracted=row["retracted"],
        retraction_notice_doi=row["retraction_notice_doi"],
        updates=tuple(WorkUpdate.model_validate(u) for u in row["updates"]),
        clinical_trial_numbers=tuple(row["clinical_trial_numbers"]),
        oa_locations=tuple(OaLocation.model_validate(o) for o in row["oa_locations"]),
        entities=tuple(EntityTag.model_validate(e) for e in row["entities"]),
        field_sources=tuple(FieldSource.model_validate(f) for f in row["field_sources"]),
        failures=tuple(
            SourceFailureView.model_validate(f) for f in (failures["failed"] if failures else [])
        ),
        enriched_at=row["enriched_at"],
    )


def record_flags(tx: Tx, project_id: Id) -> list[RecordFlag]:
    """What enrichment found for each enriched record: retractions and other notices, the
    sources that filled a field, and whether an open-access location is known."""
    rows = tx.execute(
        "select occurrence_id, retracted, updates, field_sources, oa_locations"
        " from record_enrichment where project_id = %s order by occurrence_id",
        (project_id,),
    ).fetchall()
    return [
        RecordFlag(
            occurrence_id=r["occurrence_id"],
            retracted=r["retracted"],
            updates=tuple(
                dict.fromkeys(
                    u["kind"]
                    for u in r["updates"]
                    if u["direction"] == "updated_by" and u["kind"] != "retraction"
                )
            ),
            sources=tuple(s for s in SOURCES if any(f["source"] == s for f in r["field_sources"])),
            open_access=any(o.get("format") in ("pdf", "xml") for o in r["oa_locations"]),
        )
        for r in rows
    ]


# The job


CHUNK = 50
"""Records per transaction: one batched call per source for OpenAlex, Europe PMC, and PubTator."""


def enrichment_config(ctx: AppContext) -> EnrichmentConfig:
    return EnrichmentConfig(
        contact_email=ctx.settings.contact_email,
        openalex_api_key=ctx.settings.openalex_api_key,
        semantic_scholar_api_key=ctx.settings.semantic_scholar_api_key,
        core_api_key=ctx.settings.core_api_key,
        batch_size=CHUNK,
    )


def _store_chunk(
    tx: Tx,
    run: dict[str, Any],
    result: Enrichment,
    *,
    ledger: Ledger,
) -> None:
    """Write what one chunk's lookup found, for the records no other worker wrote first. The
    caller holds the job's lease and the run's row lock."""
    done = {
        r["occurrence_id"]
        for r in tx.execute(
            "select occurrence_id from enrichment_run_record where run_id = %s"
            " and occurrence_id = any(%s)",
            (run["id"], [r.occurrence_id for r in result.records]),
        ).fetchall()
    }
    records = [r for r in result.records if r.occurrence_id not in done]
    if not records:
        return
    save_enrichment(tx, run["project_id"], tuple(records))
    rows = [
        (
            record,
            [s for s in SOURCES if s in record.sources_used],
            [f.model_dump(mode="json") for f in record.failures],
        )
        for record in records
    ]
    event = ledger.append(
        tx,
        _event(
            run["project_id"],
            RECORDS_ENRICHED,
            run["requested_by"],
            {
                "run_id": str(run["id"]),
                "records": [
                    {
                        "occurrence_id": str(record.occurrence_id),
                        "doi": record.doi,
                        "pmid": record.pmid,
                        "field_sources": [[f.field, f.source] for f in record.field_sources],
                        "retracted": record.retracted,
                        "retraction_notice_doi": record.retraction_notice_doi,
                        "failed": failed,
                    }
                    for record, _, failed in rows
                ],
            },
        ),
    )
    for record, answered, failed in rows:
        tx.execute(
            "insert into enrichment_run_record (run_id, occurrence_id, asked, answered, failed,"
            " retracted, event_id) values (%s, %s, %s, %s, %s, %s, %s)",
            (
                run["id"],
                record.occurrence_id,
                Jsonb(list(record.sources_asked)),
                Jsonb(answered),
                Jsonb(failed),
                record.retracted,
                event.event_id,
            ),
        )


def _carried(tx: Tx, run: dict[str, Any]) -> dict[Id, tuple[SourceFailure, ...]]:
    """For a retry: each record's failures from the run that enriched it last before this one."""
    rows = tx.execute(
        "select distinct on (e.occurrence_id) e.occurrence_id, e.failed"
        " from enrichment_run_record e join enrichment_run r on r.id = e.run_id"
        " where r.project_id = %s and r.id <> %s and r.requested_at <= %s"
        " order by e.occurrence_id, r.requested_at desc, r.id desc",
        (run["project_id"], run["id"], run["requested_at"]),
    ).fetchall()
    return {
        r["occurrence_id"]: tuple(SourceFailure.model_validate(f) for f in r["failed"])
        for r in rows
    }


def _private_failure(failure: SourceFailure, privacy: ContactPrivacy) -> SourceFailure:
    return failure.model_copy(
        update={
            "reason": privacy.text(failure.reason),
            "message": privacy.text(failure.message) if failure.message is not None else None,
        }
    )


def enrichment_handler(
    ctx: AppContext, gateway: Gateway, ledger: Ledger, queue: JobQueue
) -> Handler:
    config = enrichment_config(ctx)

    def handle(job: Job, job_ctx: JobContext) -> JobOutput:
        run_id = UUID(job.spec.payload["run_id"])
        with ctx.db.transaction() as tx:
            run = tx.execute("select * from enrichment_run where id = %s", (run_id,)).fetchone()
            if run is None:
                raise PermanentJobError(f"enrichment run {run_id} not found")
            done = {
                r["occurrence_id"]
                for r in tx.execute(
                    "select occurrence_id from enrichment_run_record where run_id = %s", (run_id,)
                ).fetchall()
            }
            only = (
                {UUID(o) for o in run["occurrence_ids"]}
                if run["occurrence_ids"] is not None
                else None
            )
            records = [
                r
                for r in _occurrences(tx, run["project_id"], run["identity_revision_id"])
                if r.occurrence_id not in done and (only is None or r.occurrence_id in only)
            ]
            carried = _carried(tx, run) if run["trigger"] == "retry" else {}
        for start in range(0, len(records), CHUNK):
            if job_ctx.lease_lost.is_set():
                return JobOutput()  # the heartbeat saw another worker take over; stop asking
            # The sources are asked outside any transaction; the answers are written only while
            # this worker still holds the job (`fence`), so one whose lease went to another while
            # it waited writes nothing.
            chunk = tuple(records[start : start + CHUNK])
            result = lookup(chunk, gateway, config=config, carried=carried)
            privacy = ContactPrivacy(config.contact_email)
            result = result.model_copy(
                update={
                    "failures": tuple(_private_failure(f, privacy) for f in result.failures),
                    "records": tuple(
                        r.model_copy(
                            update={
                                "failures": tuple(_private_failure(f, privacy) for f in r.failures)
                            }
                        )
                        for r in result.records
                    ),
                }
            )
            with ctx.db.transaction() as tx:
                queue.fence(tx, job)
                tx.execute("select id from enrichment_run where id = %s for update", (run_id,))
                _store_chunk(tx, run, result, ledger=ledger)

        def publish(tx: Tx) -> None:
            view = enrichment_run(tx, run["project_id"], run_id)
            ledger.append(
                tx,
                _event(
                    run["project_id"],
                    ENRICHMENT_FINISHED,
                    run["requested_by"],
                    {
                        "run_id": str(run_id),
                        "records": view.done,
                        "retracted": view.retracted,
                        "sources": [s.model_dump(mode="json") for s in view.sources],
                    },
                ),
            )
            tx.execute(
                "update enrichment_run set finished_at = %s where id = %s",
                (ctx.clock.now(), run_id),
            )
            # Crossref names the trial a paper reports: a study link for the lead to confirm.
            trial_numbers = {
                row["occurrence_id"]: row["clinical_trial_numbers"]
                for row in tx.execute(
                    "select occurrence_id, clinical_trial_numbers from record_enrichment"
                    " where project_id = %s and clinical_trial_numbers <> '[]'::jsonb",
                    (run["project_id"],),
                ).fetchall()
            }
            if trial_numbers:
                propose_study_links(
                    tx,
                    run["project_id"],
                    trial_numbers,
                    found_by="Crossref",
                    actor_id=run["requested_by"],
                    ledger=ledger,
                )
            if run["retrieve_after"] is not None:
                _retrieve_after(tx, run, ctx, ledger, queue)

        return JobOutput(publish=publish)

    return handle


def _retrieve_after(
    tx: Tx, run: dict[str, Any], ctx: AppContext, ledger: Ledger, queue: JobQueue
) -> None:
    """Queue the retrieval a retry asked for, of the reports that still lack a full text, unless a
    retrieval is going already."""
    from kittum.app.lifecycle import full_text
    from kittum.app.open_access import OPEN_ACCESS_JOB, queue_retrieval

    project_id = run["project_id"]
    lock_project_runs(tx, project_id, OPEN_ACCESS_JOB)
    going = tx.execute(
        "select 1 from retrieval_run r join job j on j.id = r.job_id where r.project_id = %s"
        " and r.finished_at is null and j.status in ('queued', 'running')",
        (project_id,),
    ).fetchone()
    wanted = [UUID(r) for r in run["retrieve_after"] if full_text(tx, project_id, r) is None]
    if going is None and wanted:
        queue_retrieval(
            tx,
            project_id,
            wanted,
            actor_id=run["requested_by"],
            trigger="retry",
            ledger=ledger,
            queue=queue,
            clock=ctx.clock,
        )


__all__ = [
    "ENRICHMENT_FINISHED",
    "ENRICHMENT_JOB",
    "ENRICHMENT_REQUESTED",
    "RECORDS_ENRICHED",
    "EnrichmentRun",
    "RecordEnrichmentView",
    "RecordFlag",
    "SourceFailureView",
    "SourceProgress",
    "enrichment_config",
    "enrichment_handler",
    "enrichment_run",
    "latest_enrichment_run",
    "lock_project_runs",
    "queue_enrichment",
    "record_enrichment",
    "record_flags",
    "request_enrichment",
]
