"""The composition root: one `AppContext` per process, built from `Settings`.

`AppContext.from_settings` wires the Postgres ledger, the job queue and command receipts, the
artifact store, the source gateway (with the default policies and the httpx transport, recording
into `Settings.gateway_record`, or the recorded responses in `Settings.gateway_recordings`), the
model backends from `Settings.llm_endpoints` (with the recipes in `Settings.recipes_file`,
recording into `Settings.llm_record` or replaced by the replays in `Settings.llm_replay`), and the
screening judge.
`job_handlers` gives a `kittum.runs.Worker` the handler of every job kind a module registered: the
screening judge, extraction runs, enrichment runs, and open-access retrieval.
"""

import functools
import logging
import os
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlsplit
from uuid import UUID

from kittum.app.enrichment import ENRICHMENT_JOB, enrichment_handler
from kittum.app.open_access import OPEN_ACCESS_JOB, open_access_handler
from kittum.app.privacy import ContactGateway, ContactPrivacy, ContactTransport
from kittum.core import Clock, KittumError, NotFoundError, SystemClock
from kittum.db import Database
from kittum.extraction import JOB_KIND as EXTRACTION_JOB
from kittum.extraction import run_handler
from kittum.gateway import (
    CONTACT_PLACEHOLDER,
    HttpxTransport,
    RecordedTransport,
    RecordingTransport,
    SourceGateway,
    default_policies,
    user_agent,
)
from kittum.judge import PROMPT_VERSION, JudgeConfig, judge_many
from kittum.ledger import PostgresLedger
from kittum.models import (
    REPLAY_PREFIX,
    LlmRecordings,
    ModelRegistry,
    RecordingBackend,
    ReplayBackend,
    backends_from_endpoints,
)
from kittum.runs import Commands, PermanentJobError, PostgresJobQueue, PostgresReceipts
from kittum.screening import JUDGE_JOB, Screening, judge_handler
from kittum.storage import ArtifactStore, FilesystemStore

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    import httpx2

    from kittum.app.assist import Assistant
    from kittum.core import Id
    from kittum.db import Tx
    from kittum.gateway import Gateway, Transport
    from kittum.ledger import Ledger
    from kittum.models import LlmBackend, ModelRecipe, RecipeBackend
    from kittum.runs import Handler, Job, JobContext, JobOutput, JobQueue, ReceiptStore
    from kittum.screening import JudgeRunner
    from kittum.settings import Settings

log = logging.getLogger(__name__)


class ConfigurationError(KittumError):
    """The settings cannot build a working process, for example a judge without a model server."""


@dataclass(frozen=True)
class Judge:
    """The screening judge this deployment runs: a config, its recipe, and the backend serving it.

    Any recipe may judge, hosted ones included: where records may go is the deployment's choice.
    The recipe and the endpoint are recorded with every verdict for the AI disclosure.
    """

    config: JudgeConfig
    recipe: ModelRecipe
    backend: LlmBackend

    @property
    def actor_ref(self) -> str:
        return f"model:{self.config.id}"

    def actor(self, tx: Tx) -> Id:
        """The model's actor row, created on first use."""
        return ensure_actor(tx, self.actor_ref, "model")

    def runner(self, *, actor_id: Id, ledger: Ledger, store: ArtifactStore, clock: Clock):
        """A `kittum.screening.JudgeRunner`. A laptop model server answers one call at a time."""
        runner: JudgeRunner = functools.partial(
            judge_many,
            actor_id=actor_id,
            config=self.config,
            recipe=self.recipe,
            backend=self.backend,
            ledger=ledger,
            store=store,
            clock=clock,
            concurrency=1 if self.recipe.serving == "ollama" else 4,
        )
        return runner


def ensure_actor(tx: Tx, identity_ref: str, kind: str) -> Id:
    row = tx.execute(
        "insert into actor (identity_ref, kind) values (%s, %s)"
        " on conflict (identity_ref) do update set kind = excluded.kind returning id",
        (identity_ref, kind),
    ).fetchone()
    assert row is not None
    return row["id"]


def judge_config(recipe_id: str, *, label: str | None = None) -> JudgeConfig:
    """The judge config for a recipe and the current prompt version."""
    return JudgeConfig(
        id=f"{label or recipe_id}@{PROMPT_VERSION}",
        model_id=recipe_id,
        prompt_version=PROMPT_VERSION,
    )


RECORDING_SECRET_LENGTH = 12
"""The shortest key or password a recording run accepts: a shorter one could stand in ordinary
text, which recording would then refuse or redact."""

# Passwords common enough to stand in ordinary text despite their length.
_COMMON_SECRETS = frozenset({"administrator", "changeme1234", "password1234", "postgrespostgres"})


def _named_secrets(settings: Settings) -> list[tuple[str, str]]:
    """Every configured key and password, with the setting it comes from."""
    password = urlsplit(settings.database_url).password
    named = [
        ("KITTUM_NCBI_API_KEY", settings.ncbi_api_key),
        ("KITTUM_OPENALEX_API_KEY", settings.openalex_api_key),
        ("KITTUM_SEMANTIC_SCHOLAR_API_KEY", settings.semantic_scholar_api_key),
        ("KITTUM_CORE_API_KEY", settings.core_api_key),
        # A DSN percent-encodes its password; the password itself is what may leak.
        ("the database password in KITTUM_DATABASE_URL", unquote(password) if password else None),
    ]
    named += [
        (e.api_key_env, os.environ.get(e.api_key_env))
        for e in settings.llm_endpoints
        if e.api_key_env
    ]
    return [(name, value) for name, value in named if value]


def recording_secrets(settings: Settings) -> tuple[str, ...]:
    """What no recording may hold: every configured key and password. The contact address is
    handled apart (it may stand in its contact parameter, as a placeholder)."""
    return tuple(dict.fromkeys(value for _, value in _named_secrets(settings)))


def check_recording_secrets(settings: Settings) -> None:
    """Refuse to record with a key or password that ordinary content could contain.

    Recording refuses a request and cleans a body that holds a secret. A short password, a common
    one, or one inside what Kittum itself sends (PubMed's `tool=kittum`, the User-Agent, the
    contact placeholder) would refuse or redact requests and bodies that hold no secret at all."""
    own = [
        s.lower()
        for s in ("kittum", user_agent(), CONTACT_PLACEHOLDER, "[redacted]", REPLAY_PREFIX)
    ]
    for name, value in _named_secrets(settings):
        folded = value.lower()
        if len(value) < RECORDING_SECRET_LENGTH:
            problem = f"of at least {RECORDING_SECRET_LENGTH} characters"
        elif folded in _COMMON_SECRETS or any(folded in v for v in own):
            problem = "that is not a common word or part of what Kittum sends"
        else:
            continue
        raise ConfigurationError(
            f"recording needs {name} to be a value {problem}, so that recording can tell it from"
            ' ordinary content; record with a longer one (README, "Record a live run, replay it")'
        )


def build_backends(
    settings: Settings,
    registry: ModelRegistry,
    artifacts: ArtifactStore,
    *,
    client: httpx2.Client | None = None,
) -> dict[str, RecipeBackend]:
    """One backend per configured endpoint, keyed by recipe ID, recording every completion into
    `Settings.llm_record`. With `Settings.llm_replay` no server is called: each configured recipe,
    and each recipe the recordings hold, is served from the recordings."""
    if settings.llm_record and settings.llm_replay:
        raise ConfigurationError("KITTUM_LLM_RECORD and KITTUM_LLM_REPLAY exclude each other")
    if settings.llm_replay:
        if not settings.llm_replay.exists():
            raise ConfigurationError(f"KITTUM_LLM_REPLAY: {settings.llm_replay} does not exist")
        recordings = LlmRecordings(settings.llm_replay)
        replays: dict[str, RecipeBackend] = {}
        for recipe_id in [e.recipe_id for e in settings.llm_endpoints] + recordings.model_ids():
            try:
                recipe = registry.get(recipe_id)
            except NotFoundError:
                raise ConfigurationError(
                    f"KITTUM_LLM_REPLAY replays {recipe_id!r}, which no recipe defines"
                    " (add it with KITTUM_RECIPES_FILE)"
                ) from None
            replays.setdefault(recipe_id, ReplayBackend(recipe, recordings, artifacts))
        return replays
    backends: dict[str, RecipeBackend] = dict(
        backends_from_endpoints(settings.llm_endpoints, registry, artifacts, client=client)
    )
    if settings.llm_record:
        check_recording_secrets(settings)
        recordings = LlmRecordings(settings.llm_record)
        backends = {
            recipe_id: RecordingBackend(
                backend,
                backend.recipe,
                recordings,
                artifacts,
                sensitive=(*recording_secrets(settings), settings.contact_email),
            )
            for recipe_id, backend in backends.items()
        }
    return backends


def build_judge(
    settings: Settings, registry: ModelRegistry, backends: Mapping[str, RecipeBackend]
) -> Judge | None:
    """The configured judge, or None when no model is configured (the API still serves)."""
    if settings.judge_backend == "demo":
        from kittum.app.demo import demo_judge

        return demo_judge(registry)
    recipe_id = settings.judge_model
    if recipe_id is None and settings.llm_replay:
        # The judge the live run used, not whichever recipe the recordings list first.
        recipe_id = LlmRecordings(settings.llm_replay).role("judge")
        if recipe_id is None and len(backends) > 1:
            raise ConfigurationError(
                f"KITTUM_LLM_REPLAY: {settings.llm_replay} does not say which recipe judged, and"
                f" it holds several ({', '.join(backends)}); set KITTUM_JUDGE_MODEL"
            )
    recipe_id = recipe_id or next(iter(backends), None)
    if recipe_id is None:
        return None
    backend = backends.get(recipe_id)
    if backend is None:
        raise ConfigurationError(
            f"KITTUM_JUDGE_MODEL is {recipe_id}, but no KITTUM_LLM_ENDPOINTS entry serves it"
        )
    if settings.llm_record:
        LlmRecordings(settings.llm_record).set_role("judge", recipe_id)
    return Judge(config=judge_config(recipe_id), recipe=backend.recipe, backend=backend)


def build_gateway(
    settings: Settings,
    db: Database,
    artifacts: ArtifactStore,
    clock: Clock,
    *,
    transport: Transport | None = None,
) -> SourceGateway:
    """The source gateway: live over HTTP (`transport`, the httpx one by default), or, in offline
    mode only, from recorded responses with a cache and rate limits of its own process, so
    recordings never mix with what the sources said. Recordings without offline mode, or offline
    mode without recordings, are refused.

    Recording (`Settings.gateway_record`) is live only, and caches in memory: every request of the
    run reaches its source once and is written, even one the deployment's cache could answer."""
    if bool(settings.gateway_recordings) != settings.offline:
        raise ConfigurationError(
            "KITTUM_GATEWAY_RECORDINGS and KITTUM_OFFLINE=1 go together: recordings replace the"
            " real sources only in offline mode, and offline mode needs recordings"
        )
    if settings.gateway_record and settings.offline:
        raise ConfigurationError(
            "KITTUM_GATEWAY_RECORD records the real sources, so it cannot run with KITTUM_OFFLINE=1"
        )
    if settings.gateway_record:
        check_recording_secrets(settings)
    if settings.gateway_recordings:
        transport = RecordedTransport(
            settings.gateway_recordings, contact_email=settings.contact_email
        )
    else:
        transport = transport or HttpxTransport(contact_email=settings.contact_email)
        if settings.gateway_record:
            transport = RecordingTransport(
                transport,
                settings.gateway_record,
                contact_email=settings.contact_email,
                secrets=recording_secrets(settings),
            )
    in_memory = settings.offline or settings.gateway_record is not None
    privacy = ContactPrivacy(settings.contact_email)
    return ContactGateway(
        privacy=privacy,
        database=db,
        transport=ContactTransport(transport, privacy),
        buckets="memory" if settings.offline else "postgres",
        cache="memory" if in_memory else "postgres",
        artifacts=artifacts,
        policies=default_policies(
            contact_email=settings.contact_email, ncbi_api_key=settings.ncbi_api_key
        ),
        clock=clock,
    )


@dataclass
class AppContext:
    """What routes, commands, and workers may use. Built once per process; tests build their own."""

    settings: Settings
    db: Database
    artifacts: ArtifactStore
    clock: Clock = field(default_factory=SystemClock)
    gateway: Gateway | None = None
    ledger: Ledger | None = None
    queue: JobQueue | None = None
    receipts: ReceiptStore | None = None
    backends: Mapping[str, LlmBackend] = field(default_factory=dict)
    """Every configured model server, by recipe ID (`Settings.llm_endpoints`)."""
    judge: Judge | None = None
    assistant: Assistant | None = None
    """The protocol assistant's model and count gateway (`kittum.app.assist`)."""

    @classmethod
    def from_settings(cls, settings: Settings) -> AppContext:
        clock = SystemClock()
        db = Database(settings.database_url)
        artifacts = FilesystemStore(settings.artifact_dir)
        from kittum.app.assist import build_assistant

        registry = ModelRegistry.load(local=settings.recipes_file)
        backends = build_backends(settings, registry, artifacts)
        gateway = build_gateway(settings, db, artifacts, clock)
        return cls(
            settings=settings,
            db=db,
            artifacts=artifacts,
            clock=clock,
            gateway=gateway,
            ledger=PostgresLedger(clock),
            queue=PostgresJobQueue(clock),
            receipts=PostgresReceipts(),
            backends=backends,
            judge=build_judge(settings, registry, backends),
            assistant=build_assistant(settings, backends, artifacts),
        )

    def require_ledger(self) -> Ledger:
        if self.ledger is None:
            raise ConfigurationError("no ledger is configured")
        return self.ledger

    def screening(self) -> Screening:
        return Screening.postgres(self.require_ledger(), self.clock)


type HandlerFactory = Callable[[AppContext], Handler | None]
"""Builds a job kind's handler for a worker, or None when the context cannot run it."""

JOB_HANDLERS: dict[str, HandlerFactory] = {}


def register_job_handler(kind: str, factory: HandlerFactory) -> None:
    """Add a job kind to `kittum worker`. Extraction (WP-11) registers its jobs here."""
    if kind in JOB_HANDLERS and JOB_HANDLERS[kind] is not factory:
        raise ValueError(f"a different handler for {kind!r} is already registered")
    JOB_HANDLERS[kind] = factory


def _judge_jobs(ctx: AppContext) -> Handler | None:
    if ctx.judge is None:
        log.warning("no judge is configured, so this worker runs no %s jobs", JUDGE_JOB)
        return None
    with ctx.db.transaction() as tx:
        actor_id = ctx.judge.actor(tx)
    runner = ctx.judge.runner(
        actor_id=actor_id, ledger=ctx.require_ledger(), store=ctx.artifacts, clock=ctx.clock
    )
    screening = ctx.screening()
    inner = judge_handler(ctx.db, screening, runner, actor_id=actor_id)
    config_id = ctx.judge.config.id

    def handle(job: Job, job_ctx: JobContext) -> JobOutput:
        # A stage names its judge; refuse before any model call rather than judge with another.
        with ctx.db.transaction() as tx:
            info = screening.stage(tx, job.spec.project_id, UUID(job.spec.payload["stage_id"]))
        if info.judge_config_id != config_id:
            raise PermanentJobError(
                f"this worker runs judge {config_id}; the stage needs {info.judge_config_id}"
            )
        return inner(job, job_ctx)

    return handle


register_job_handler(JUDGE_JOB, _judge_jobs)


def _extraction_jobs(ctx: AppContext) -> Handler | None:
    if ctx.queue is None or ctx.receipts is None:
        log.warning("no job queue is configured, so this worker runs no %s jobs", EXTRACTION_JOB)
        return None
    # Without a model server a run still takes the Janus path; one that asks for the agent gets
    # its LLM proposals recorded as errors naming the missing backend.
    commands = Commands(ctx.require_ledger(), ctx.queue, ctx.receipts, ctx.clock)
    return run_handler(
        db=ctx.db,
        artifacts=ctx.artifacts,
        commands=commands,
        backends=ctx.backends,
        clock=ctx.clock,
    )


register_job_handler(EXTRACTION_JOB, _extraction_jobs)


def _source_jobs(
    build: Callable[[AppContext, Gateway, Ledger, JobQueue], Handler], kind: str
) -> HandlerFactory:
    """Jobs that reach external sources: only a context with a gateway and a queue runs them."""

    def factory(ctx: AppContext) -> Handler | None:
        if ctx.gateway is None or ctx.queue is None:
            log.warning("no source gateway or job queue, so this worker runs no %s jobs", kind)
            return None
        return build(ctx, ctx.gateway, ctx.require_ledger(), ctx.queue)

    return factory


register_job_handler(ENRICHMENT_JOB, _source_jobs(enrichment_handler, ENRICHMENT_JOB))
register_job_handler(OPEN_ACCESS_JOB, _source_jobs(open_access_handler, OPEN_ACCESS_JOB))


def job_handlers(ctx: AppContext) -> dict[str, Handler]:
    """Every registered job kind this context can run."""
    handlers = {kind: factory(ctx) for kind, factory in JOB_HANDLERS.items()}
    privacy = ContactPrivacy(ctx.settings.contact_email)

    def protect(handler: Handler) -> Handler:
        guarded = privacy.guard(handler)

        def run(job: Job, job_ctx: JobContext) -> JobOutput:
            output = guarded(job, job_ctx)
            return (
                replace(output, publish=privacy.guard(output.publish))
                if output.publish is not None
                else output
            )

        return run

    return {kind: protect(handler) for kind, handler in handlers.items() if handler is not None}


__all__ = [
    "JOB_HANDLERS",
    "RECORDING_SECRET_LENGTH",
    "AppContext",
    "ConfigurationError",
    "HandlerFactory",
    "Judge",
    "build_backends",
    "build_gateway",
    "build_judge",
    "check_recording_secrets",
    "ensure_actor",
    "job_handlers",
    "judge_config",
    "register_job_handler",
]
