# Work packages

Each package is one module or one cross-cutting piece, sized for one agent and one branch. Every
interface exists as a stub, and the shared fakes exist, so all packages can start now. "Needs"
lists what a package calls at integration time, not what it waits for.

The two-week spike in [DESIGN.md](../DESIGN.md#two-week-spike) picks from these in order: WP-01,
02, 03, 05, 08, 13, then 09, 10, 11, 14.

## Status

| WP | Module | Status |
| --- | --- | --- |
| 00 | Scaffold: `core`, layout, CI, compose, contracts pipeline | Done |
| 01 | `ledger` | Done: Postgres and SQLite stores, checkpoints, `seal`, `kittum verify`; TSA and WORM copies open |
| 02 | `runs` | Done: job queue, worker, receipts, decider helper, fakes; run manifests and `diff` open |
| 03 | `protocol`, `profiles` | Done: schema, profiles, compile, diff, rendering, persistence, routes |
| 04 | `gateway` | Done: httpx transport, policies, shared buckets, budgets, cache, attempts; AppContext wiring open |
| 05 | `search` | Done |
| 06 | `enrich` | Clients, stored enrichment, full-text waterfall, and jobs in the app done; local tagging open |
| 07 | `corpus` | Dedupe, link proposals, immutable identity revisions, PRISMA counts done |
| 08 | `judge` and `models` | Done; ledger store and job wiring wait for WP-01 and WP-02 |
| 09 | `screening` | Done: M1 to M3, blinding, reveal, sentinels, audits, adjudication, judge jobs, metrics, routes; open items listed |
| 10 | `documents` | Parsers, layers, anchors, re-anchoring, viewer routes, and acquisition done; source resolvers for the waterfall open |
| 11 | `extraction` | Done: schema, both paths, validators, reconciliation, runs as jobs, acceptance, bundle exporter, routes, workbench screen; open items listed |
| 12 | `appraisal` | Types done; open |
| 13 | `stats` | Done: intervals, stopping, capture-recapture, conformal threshold, agreement |
| 14 | `report` | Done: PRISMA flow, RIS, search report, AI disclosure, bundle exporters and `seal_bundle`, `gba_amnog` targets, routes; open items listed |
| 15 | `api`, `app` | Done: composition root, project lifecycle (service, routes, CLI), `serve` with the workbench, `worker`, `demo seed`; authentication open |
| 16 | Workbench | Done: wired to the API with a reviewer picker; setup from an empty review (people, protocol editor, searches, corpus, stages, full texts); screening, full text, adjudication, progress, protocol, search, studies, extraction runs and review, report rendering and sealing; end-to-end journeys in CI |
| 17 | Evaluation harness | Open |

## WP-01 Ledger

Module `kittum.ledger`. Needs: `core`.

- A Postgres store on the foundation tables: append with per-project serialization (advisory lock
  or `select ... for update` on the chain head), events, and verify.
- A SQLite store with the same behaviour, for the handover bundle and tests.
- Signed checkpoints (Ed25519 with `cryptography`), with the public key ID recorded.
- `seal(cutoff_seq)`: the bundle manifest and the offline `verify` command.
- Acceptance: both stores pass one shared test suite. Verify catches a changed payload, a removed
  event, a reordered event, and a cut tail after a checkpoint.

**Done.** `PostgresLedger`, `SqliteLedger`, and `InMemoryLedger` share one implementation of the
chain rules (`ledger/store.py`); `tests/ledger/test_stores.py` runs one suite over all three.

- Postgres appends take `pg_advisory_xact_lock` on the project, held until the caller commits.
- `checkpoint(tx, project_id, signer)` verifies the chain, signs the head with Ed25519, and stores
  the row. `verify(tx, project_id, checkpoint=..., key=...)` catches a cut tail and a chain
  rewritten from some point on, which the chain alone cannot. `VerifyReport.problem` names what
  failed. The key comes from `KITTUM_CHECKPOINT_KEY` (`Settings.checkpoint_key_path`); the key ID
  is the SHA-256 of the raw public key.
- `seal(tx, project_id, cutoff_seq, store, out_dir, signer=..., actor_id=...)` writes
  `kittum.sqlite` (ledger through the cutoff, checkpoints, artifact index), `artifacts/`, and a
  signed `manifest.json`, and records a `ledger.bundle_sealed` event. Modules add tables and
  artifacts with `register_exporter(exporter)`; a withheld artifact is indexed by hash with a
  reason, and a withhold wins over an include.
- `kittum verify <bundle> [--public-key pem]` checks offline: the manifest signature, the
  checkpoint signature, chain continuity to the cutoff, file hashes, unlisted files, and the
  artifact index. Without `--public-key` it uses the bundle's own key and says the key is not
  pinned.

Left open:

- RFC 3161 timestamps over checkpoints (`tsa_token_hash` stays empty) and copies of checkpoints
  outside the database (WORM storage). Until then, pass `verify` a checkpoint kept elsewhere.
- Scheduled checkpoints at stage gates and daily, once `runs` and the stage gates exist.
- The `checkpoint` table has no append-only trigger, because it is in the foundation migration.
  A trusted external copy covers this; a trigger would be a small foundation change.
- Bundle contents from DESIGN.md beyond the ledger: `dossier/`, `registry/`, count
  reconciliation, and the "every accepted value has an anchor" check belong to the exporters of
  `report`, `extraction`, and `documents`.

## WP-02 Runs and jobs

Module `kittum.runs`. Needs: `ledger`.

- `JobQueue` on the `job` table with `for update skip locked`, leases, heartbeats, retries with
  backoff, and dead-lettering. Unique `stage_key` prevents double publication.
- Command receipts: a retried command returns the stored receipt and writes nothing.
- The decider pattern: a helper that commits events, rows, receipt, and jobs in one transaction.
- A worker loop with graceful shutdown, and an in-memory queue fake.
- Acceptance: two workers never run the same job; a crashed worker's job is reclaimed after its
  lease; a replayed command is a no-op.

**Done.** `tests/runs/test_queue.py` runs one suite over `PostgresJobQueue` and the
`InMemoryJobQueue` fake.

- `JobQueue` methods take the caller's `tx`. A claimed `Job` is the lease token: `heartbeat`,
  `complete`, and `fail` check owner and attempt and raise `LeaseLost` otherwise. Expired leases
  are reclaimed; an expired lease on the last attempt dead-letters the job.
- `fail` requeues with exponential backoff and jitter (`RetryPolicy`), and marks the job `dead`
  after `max_attempts`; `retryable=False` (or `PermanentJobError` in a handler) marks it `failed`.
- `enqueue` with an existing `stage_key` returns the existing job, or raises `StageKeyConflict` if
  the inputs differ.
- `Commands(ledger, queue, receipts).execute(tx, command, decide, load=..., apply=...)` locks the
  command ID, returns a stored receipt unchanged, or decides, appends events, applies rows,
  enqueues jobs, and saves the receipt in one transaction.
- `Worker(db, queue, handlers, name=...)` claims, heartbeats in a thread, and completes the job in
  the same transaction as the handler's `JobOutput.publish`. `stop()` (or `stop_on_signals()`)
  lets the current job finish. Fakes: `InMemoryJobQueue`, `InMemoryReceipts`, `NullTransactions`.

Left open:

- Run manifests, cache keys, branch runs, and `runs.diff` (DESIGN.md "Runs and reproducibility").
- A `kittum worker` entry point that builds handlers from the modules.
- Receipts do not store a command hash, so a reused command ID with a different payload but the
  same type returns the old receipt. Detecting that needs a column on `command_receipt`.

## WP-03 Protocol

Module `kittum.protocol` (domain-neutral core) and `kittum.profiles` (review-community profiles).
Needs: `core`, `db`, `ledger`.

Done:

- A generic schema: review questions (each a scope) with PICO, comparators (optionally
  alternatives), and outcome IDs; outcomes with a category; effect modifiers; search topics with
  criteria, source plan, reference set, screening mode with audit and stopping plans, and a
  freshness policy. All human text is localized (`{en: ..., de: ...}`) with a required
  `default_language`.
- Profiles: `register_profile(Profile)` adds extension schemas (protocol, question, comparator,
  outcome, topic, criterion), vocabularies (`outcome_category`, `topic_kind`), rules, labels, and
  default effect modifiers. The core never imports a profile; `api.create_app` calls
  `kittum.profiles.load_profiles()`.
- `kittum.profiles.gba_amnog`: dossier and Anwendungsgebiete, Teilpopulationen with zVT
  (comparators as alternatives, "nach ärztlicher Maßgabe", off-label options), JCA PICOs as linked
  questions, the four AMNOG endpoint categories, Modul 4 search kinds, the three-month search age,
  the Modul 4 source minimum, German as the default language.
- `compile(source)` from YAML, JSON, or a mapping: every issue with its path, duplicate YAML keys
  with line numbers, cross-checks (unique IDs, scopes exist, every topic and scope decidable at
  title and abstract, mode rules, reference identifiers), and a content hash over RFC 8785 JSON.
- `diff(a, b)`: added, removed, and changed criteria and scopes, mode changes, and field-level
  changes keyed by ID.
- `render_question` (reviewer question with answer keys, profile slot labels) and
  `render_prompt` (judge fragment asking for a verbatim quote, languages in stable order).
- Persistence in `protocol_version` (migration `20261028140000_protocol_versions.sql`):
  `save_draft` (idempotent per content hash), `publish` (human approver, `protocol.published`
  ledger event, reason required for a mode change, then frozen by trigger), `get_version`,
  `list_versions`, `latest_published`. `testing.InMemoryProtocolStore` is the fake.
- Routes: `POST /api/protocol/validate`, `POST /api/projects/{id}/protocol-versions`,
  `POST .../{version_id}/publish`, `GET .../protocol-versions`, `GET .../{version_id}`.
- Examples: `examples/protocols/capivasertib.yaml` (G-BA D-1110, `gba_amnog`) and
  `examples/protocols/nicotine-replacement.yaml` (no profile).

Left:

- Swap the test-only `SqlLedger` (tests/protocol/support.py) for the WP-01 Postgres ledger, and
  give `AppContext.ledger` a real default once it exists.
- Approver identity comes from the request body until the API has authentication (WP-15).
- A profile for EU JCA dossiers as a report target, if JCA needs more than linked questions.

## Protocol assistant

A model helps the methods lead write the protocol: `kittum.assist`, `app/assist.py`,
`api/routes/assist.py`, and the protocol form (`screens/protocol/Assistant.tsx`).

Done:

- Sources carry an optional `strategy`, left out of the document when absent so older protocols
  keep their content hash. Compile checks quotes, parentheses, and brackets (`strategy_problems`);
  a PubMed field tag outside PubMed's documented list, each confirmed against ESearch, is a
  warning (`strategy_warnings`, `protocol_warnings`, `ValidationReport.warnings`);
  `strategy_dialect` names PubMed or ClinicalTrials.gov.
- `draft` and `improve` (a question, a criterion, or a strategy as find-and-replace changes) ask
  the model in a strict JSON Schema derived from the protocol's models (`strict_schema`,
  `from_strict`), compile every answer, and send problems back at most twice (`REPAIRS`).
- `record` stores a compiled answer as a `draft` or `suggestion` revision and writes a
  `protocol.assist` ledger event with every attempt, also for an answer that never compiled.
  `accept` (some changes), `restore`, and `save_edit` add revisions; `assist_revision` is
  append-only.
- AI marks by value: a field is the model's while its value is one a model wrote in the
  project's assistant history (draft, suggestion, or accepted selection), whichever way the
  document was saved. Free text (a string field of the schema without enumeration, constant, or
  pattern) matches by kind of field, its path without object IDs, so renamed and moved objects
  keep their marks; choices (enums, a source's name and platform), numbers, and booleans match at
  the exact path, or in an object corresponding to one a model wrote (same kind of object, some
  free text of its own equal), so a renamed topic keeps its mode and numbers; IDs are never
  marked. Entries of lists without IDs (reference sets, deviations) are fields by position. `kittum.protocol.publish` asks every registered `Attribution`; importing
  `kittum.assist` registers the assistant's, so publications from the workbench, an assistant
  revision, and the CLI all record `ai_parts`. With none registered, publishing a project that has
  assistant revisions raises `ProtocolStateError`. The `ai_disclosure` report lists them with every assistant request.
- Improving a question or criterion keeps every nested profile extension (by ID); a language given
  twice in an answer goes back to the model; a model server that is down or fails is a recorded
  attempt with the bytes that exist.
- `kittum.search.count_hits`: ESearch count, with every phrase PubMed found nowhere (quoted or
  not), ignored, or could not field, or ClinicalTrials.gov `totalCount`, through the gateway
  (source `clinicaltrials`, the gateway policy's name, which the CT.gov search now uses too).
- Settings: `KITTUM_ASSIST_MODEL`, `KITTUM_ASSIST_BACKEND` (`demo` replays recorded answers).
  `KITTUM_LLM_RECORD` notes the assistant's recipe as the `assist` role of its recordings; a
  `KITTUM_LLM_REPLAY` without `KITTUM_ASSIST_MODEL` uses it, and refuses recordings of several
  recipes that do not name it. `KITTUM_ASSIST_BACKEND=demo` is refused beside `KITTUM_LLM_REPLAY`.
- The form: a lead without a model fills in an empty protocol or the published one, edits the
  questions, criteria, sources, and queries in place, and saves each change as a revision under
  their name (`save_edit`), with problems shown at their fields. A query counts before it is
  saved. The YAML tab shows the same protocol (`POST /api/protocol/yaml`) and hands its edit back.
  The history names each revision's author; `,` and `.` step through it and change nothing;
  **Restore this revision** (`r`) adds a copy under the lead's name. `GET .../assist/model` names
  the model, so the lead sees what Kittum waits for, and for how long.
- A count or live search whose connection fails or times out is a 502 with a plain reason
  (`kittum.search.unreachable`, `failure_message`), as an unreachable source is, never a 500. The
  model client waits up to 15 minutes for one answer.
- Tests: `tests/assist/test_assist.py` on GLM-5.3-Flash's recorded answers and PubMed's recorded
  counts; `tests/search/test_routes.py` for connections that fail; `e2e/assist.spec.ts` in the
  browser, with the model and by hand.

Left:

- Model calls run in the request; a model that reasons at length holds it for minutes (no layer
  times out before 15). A job would free it.
- The form edits one search topic's sources and criteria as it finds them; adding a search topic,
  outcome definitions, reference sets, and profile extensions are edited as YAML.
- Strategies for platforms other than PubMed, Europe PMC, and ClinicalTrials.gov are not
  syntax-checked or counted. The draft prompt asks for PubMed and ClinicalTrials.gov only, so
  GLM's recorded drafts still replay; a Europe PMC strategy gets Europe PMC's syntax in its
  improve request.

## WP-04 Gateway

Module `kittum.gateway`. Needs: `core`.

- Done: `HttpxTransport` on httpx2, with timeouts, redirects, and a `User-Agent` naming the
  Kittum version and the configured contact email.
- Done: default policies for PubMed, ClinicalTrials.gov, Europe PMC, OpenAlex, Crossref,
  Unpaywall, and PubTator, 10% under docs/sources.md (`default_policies()`); a caller overrides
  any of them by passing its own mapping. A contact email moves Crossref to the polite pool and
  an NCBI key raises PubMed's rate.
- Done: `SourceGateway` with a pyrate-limiter bucket per source. The Postgres bucket shares the
  limit across processes; `buckets="memory"` is for tests. One table per source, named
  `ratelimit___gateway_<source>`. A per-source concurrency cap is enforced with Postgres advisory
  slots held for each send, so it spans workers too.
- Done: daily budgets in `gateway_budget`, consumed per attempt, so a new gateway instance
  continues where the previous one stopped. Exhaustion raises `SourceBudgetExhausted`; the
  caller decides whether to wait for the next day or fail the job.
- Done: `Retry-After` (seconds or HTTP date) and `X-RateLimit-Remaining`/`X-RateLimit-Reset`
  pause a source before the next attempt, with the full deadline in `gateway_cooldown`, so every
  worker waits. Retries with stamina on 429, 502, 503, and 504, five attempts, exponential
  backoff with full jitter.
- Done: content-addressed cache: the body goes to `ArtifactStore`, `gateway_cache` maps the
  canonical request hash to status, headers, and body hash. `use_cache=False` skips the read but
  stores the fresh response for later replays. Every transport attempt lands in `gateway_attempt`.
- Done: `FakeTransport` scripts a sequence of responses (`kittum.gateway.testing`).
- Left: entry points do not build a gateway yet. `AppContext` needs a `gateway` field (an
  interface change outside this module) before search and enrichment can call it.
- Left: PubMed's and OpenAlex's API keys are not attached to requests here; adapters pass them
  as request parameters. Retry-After on 503 is honoured, but there is no circuit breaker or
  source-health memory beyond the cooldown deadlines.
- Acceptance met: tests against real Postgres with `FakeTransport` show a 429 then 200 with both
  attempts recorded, five failures with a clear error, a cache hit with no transport call,
  independent buckets for two sources, a budget that survives a new gateway, the concurrency cap,
  and cooldowns shared across gateway instances.

## WP-05 Search

Module `kittum.search`. Needs: `gateway`, `protocol`, `ledger`.

- PubMed E-utilities: ESearch with history, date windows under the 10,000 cap, EFetch, query
  translation capture, structured abstracts with labels kept, registry IDs from `DataBankList` and abstract.
- ClinicalTrials.gov API v2 with Essie syntax, pagination across multiple pages, mapping to NCT ID, status, and phase.
- Europe PMC REST search with `synonym=FALSE` and `cursorMark` paging; records keep their PMID,
  PMCID, and DOI, and NCT IDs from title and abstract. The query translation records the query
  Europe PMC ran and its synonym setting. Fewer records than `hitCount` fails the run.
  `live_source` starts PubMed, Europe PMC, or ClinicalTrials.gov by name for the API and the CLI.
- PubMed records keep their PMCID too (`source_occurrence.pmcid`).
- File imports: RIS (Generic, Cochrane Library CENTRAL, Embase) and CSV (CTIS, EU-CTR).
- Search execution and source occurrence persistence in Postgres with audit ledger event (`search.executed`).
- HTTP API routes (`/api/projects/{project_id}/search/*`) registered and contract generated.
- `FakeSource` in `search/testing.py`.
- Acceptance: recorded real responses in `tests/search/fixtures/` reproduce counts, query translation, and PRISMA-S fields; tests pass end-to-end without network.
- `run` and `import_file` take the caller's ledger, actor, and artifact store. Occurrence IDs are
  assigned at persistence; every raw hash must be in the store before rows are written; the
  `search.executed` payload is built once (`prisma_s_payload`). Routes take `actor_id` in the body
  and answer 503 without a ledger or gateway.
- PubMed date windows span 1000/01/01 to 3000/12/31 and must cover the original count; PMIDs are
  deduplicated across windows, and unique articles plus skipped book records must equal ESearch's
  `Count`, or the run fails. CSV imports parse strictly and reject rows wider or narrower than the
  header.
- Left: take the actor from the session once auth exists; run live searches as a job (the HTTP calls
  currently happen inside the route's transaction); map `PubmedBookArticle` records (today they are
  skipped and listed in `skipped_record_ids`).

## WP-06 Enrichment

Module `kittum.enrich`. Needs: `gateway`, `search`, `models`.

- Done: OpenAlex (works by DOI and PMID in OR batches, inverted-index abstracts, OA locations),
  Crossref (`update-to`, `updated-by`, and `relation` notices, `clinical-trial-number`, polite
  pool), Europe PMC (batched ID search, `synonym=FALSE`, full-text availability), Unpaywall (best
  OA location and licence), and PubTator 3 (BioC JSON entities re-anchored in the title plus
  abstract).
- Done: "first healthy source" per field. A source that errors or is out of budget is recorded in
  `Enrichment.failures` and the next one answers; `FieldSource` records which source supplied each
  field. Identifier lookups batch where the API allows.
- Done: `enrich(tx, project_id, records, gateway)` writes one `record_enrichment` row per
  occurrence, replacing an earlier row for the same occurrence. `EnrichmentConfig` carries the
  contact email and OpenAlex key that entry points map from `Settings`.
- Done: `full_text_candidates` lists Europe PMC XML and PDF, then Unpaywall, then OpenAlex file
  URLs, each with its licence.
- Done: recorded real responses in `tests/enrich/fixtures/`, including a retracted paper whose
  Crossref notice DOI is flagged.
- Done: Crossref's openly licensed publisher PDF links (`link` with a Creative Commons `license`
  for its content version, or for text and data mining) are full-text locations, last in the
  waterfall. PubTator's source name is the gateway policy's (`pubtator`); as `pubtator3` every
  call through the real gateway failed as an unknown source. `SOURCES` and `sources_asked` say
  which sources `enrich` asks about a record, for progress per source.
- Open: local entity tagging with the OpenMed GLiNER models, for text that must stay local.

## WP-07 Corpus

Module `kittum.corpus`. Needs: `search`, `enrich`, `ledger`.

- Dedupe rules over `SourceRecord`s: report identifiers (exact DOI, PMID) and rapidfuzz normalized title plus year and first author surname, keeping every occurrence.
- Trial identifiers (NCT, EudraCT, CTIS numbers) drive only `same_study` proposals and never merge records or reports; distinct publications of one trial with the same NCT number stay separate reports linked to one study.
- Link proposals for report (`same_record`, `same_report`) and study (`same_study`) with evidence strings.
- Immutable identity revisions in Postgres (`report`, `study`, `corpus_revision`, `identity_revision`, `identity_link`, `proposal` tables with migration `20261028020000_corpus_identities.sql`). A revision is written with its links in one transaction and then finalized; triggers reject any later update, delete, or added link.
- Merge and confirm move whole groups: `same_record`/`same_report` union complete report groups (and their studies), `same_study` unions complete study groups. A stronger confirmed identity upgrades the chosen occurrences' labels and never downgrades one. A split names its level: `report` moves occurrences into new reports of the same study, `study` moves complete report groups into new studies.
- A record without identifiers never bridges two different DOIs or PMIDs into one proposal; titles conflict, and are never fuzzy-matched, only when both name a trial (an acronym with two capitals, or a word both titles put before a number, other than common words such as stage or cohort), phase, follow-up length, or study number and the values differ (KEYNOTE 024 vs 042, 5-year vs 3-year); a number on one side only, such as a citation's year or pages, never blocks a match.
- Ledger integration: human confirmation, split, and merge write audit events (`corpus.confirmed`, `corpus.merged`, `corpus.split`) via `Ledger.append(tx, EventDraft)` to the hash-chained ledger.
- PRISMA 2020 identification counts from a frozen identity revision: records identified per source, duplicates removed, unique records, studies identified.
- HTTP routes in `src/kittum/api/routes/corpus.py`: list proposals, confirm, merge, split, get counts, all scoped to the project (404 for another project's revision, 400 for invalid merges and splits).
- `FakeCorpus` in `src/kittum/corpus/testing.py` for downstream modules.
- End-to-end tests against Postgres verifying PRISMA counts, revision immutability, history preservation on split, and separate reports for same-NCT publications.
- Report identifiers include the PMCID. A `same_record` proposal joined by identifiers alone is
  `exact`; `confirm_exact` (`POST corpus/confirm-exact`, key `a` on Studies) confirms them all, one
  revision each. Evidence reads as the lead sees it ("Same DOI ...", "Same NCT ID ...").
- `propose_study_links` adds study-link proposals from registry IDs found after ingest; a finished
  enrichment run passes Crossref's `clinical-trial-number`s (`corpus.proposed`).
- The corpus view names each record's search (`search_ids`, `searches`) and why linked records are
  together (`links`: the confirmed proposal's evidence or the lead's reason for a merge by hand,
  for the records still together). `tests/corpus/test_duplicates.py` and
  `e2e/duplicates.spec.ts` run PubMed, Europe PMC, and ClinicalTrials.gov on recorded responses.
- Left: enrichment fills a missing DOI or PMID, but deduplication runs once at ingest and does not
  see it; ClinicalTrials.gov's `referencesModule` PMIDs are not used for study links.

## WP-08 Judge and models

Modules `kittum.judge` and `kittum.models`. Needs: `protocol`, `core`.

- An OpenAI-compatible backend for vLLM and Ollama with JSON Schema-constrained output; the model
  recipe registry with the licence allowlist; where each call went (serving and endpoint).
- `judge`: the prompt per criterion set, decoding, `find_quote` verification, keyword and negation
  checks, downgrade to `unclear`, raw output capture.
- Gold-data metrics: per-judge recall and specificity, the double-fault rate, run-to-run kappa.
- Acceptance: recorded completions cover verified, unverified, paraphrased, and negated quotes.

**Done.**

- `models.OpenAICompatibleBackend`: `response_format` with `json_schema`, temperature 0, a seed,
  the recipe's `extra_body` (thinking switches), raw request and response bytes in the artifact
  store, the model identity the server reports, thinking removed before the JSON (`<think>`,
  Gemma channels, harmony, code fences, or the `reasoning` field), retries on transport errors
  only. The answer streams as server-sent events, so the read timeout applies between chunks; the
  stored response is the event stream as received, and `models.assemble` reads it back. A
  recipe's `reasoning_tokens` is added to every call's `max_tokens`, outside the request key; an
  answer that ends at the budget (`finish_reason` "length") is not asked for again (`models.cut_off`).
  `backends_from_endpoints` builds backends from `Settings.llm_endpoints`. A hosted API's key comes
  from the environment variable `KITTUM_LLM_API_KEYS` names, read for each call and never stored;
  each completion records the recipe's serving and the endpoint (without credentials). Kittum
  does not gate hosted models: where content goes is the operator's decision.
- `models.ModelRegistry` loads `models/recipes.yaml`: Qwen3.8-27B (thinking off and on),
  Gemma-4-31B-it, Nemotron 3.5 Super VL NVFP4, and Gemma 4 E4B, Gemma 4 12B, and Qwen3.5-4B on
  Ollama. Revisions and weight hashes were checked against Hugging Face and the Ollama registry on
  28.10.2026. Loading rejects licences outside `ALLOWED_WEIGHTS_LICENCES`.
  `ModelRegistry.load(local=...)` adds a deployment's own recipes (`KITTUM_RECIPES_FILE`, same
  format); a local ID may not reuse a shipped one, and the allowlist applies. Hosted recipes pin
  no weight files.
- `judge.judge` and `judge.judge_many`: one call per record and scope, a per-criterion schema
  generated from Pydantic, verification and downgrades with `failed_check`, retry with the next
  seed on malformed output, then `unclear` with the `invalid_output` flag. Records that try to
  instruct the model cannot be excluded. A cheap, conservative filter (`judge/verify.py`)
  downgrades `not_met` answers whose evidence is not structurally supported: one sentence, a
  whole-word key term without a negating compound prefix, an English or German quote, at most one
  negation cue and it scoped to the key term, and the right polarity for an include or exclude
  criterion. The negation and design checks read the criterion (#44): a negation of one
  requirement of an affirmative condition ("adults ... who did not smoke tobacco" against
  "Adults who smoke tobacco") and, on a study design criterion, another design named by a
  keyword as the object of the record's own description ("This was a prospective cohort study",
  or the title's "...: a prospective cohort study" against "Randomized controlled trial", in a
  record with no hint of random allocation) support `not_met`. Any negated quote that excludes
  is read with its whole sentence: nothing but closing punctuation follows its last negated
  term, the sentence names no time, frequency, or age and coordinates or compares no other
  population, and no earlier sentence says who was excluded, across blank lines and list
  items, unless a sentence stating who was enrolled or a section label comes between. A bare
  survey, questionnaire, interview, or assessment is a measurement, not a design.
  `Verdict.excludes` is a proposal for humans and adjudication, never a final exclusion.
  `judge_many` stores verdicts (migration `20261028120000_judge_verdicts.sql`), artifacts, and
  `judge.verdict_recorded` events, and skips records already judged under the same cache key.
- `judge.judge_metrics`, `pair_metrics` (double-fault rate, OR rule, Cohen's kappa), and
  `fleiss_kappa` for run-to-run agreement.
- A small gold set (`tests/judge/fixtures/demo_gold.json`): the demo's 30 title and abstract
  items, the screeners' decisions from the end-to-end plan, and recorded answers from
  GLM-5.3-Flash and qwen3.5:4b. The filter verifies 2 of GLM's 5 correct `not_met` readings (was
  0) and 2 of qwen's 5 (was 1), and none that the screeners contradict. GLM's "This ComBaCaL
  cohort study" stays `unclear`: the record also mentions "nested trials".

**Left.**

- `judge_many` holds the caller's transaction across model calls. When WP-02 lands, split it into
  a job that calls the model and a command that records the verdict.
- The judge is built against WP-03's `Criterion` (`text` per language, `polarity`, `keywords`)
  and renders criteria with `kittum.protocol.render_prompt`. This branch carries a temporary copy
  of those types in `protocol/__init__.py`; drop it when the protocol branch is merged.
- Ollama reports only the tag as the model identity. Record the blob digest from `/api/show` if
  audits need more.
- Trade-off, accepted for recall: a quote that contradicts a criterion by value without naming
  one of its terms ("patients aged 12 to 17 years" against "adults") is `unclear`, as are
  exclusions on mixed conditions ("adults without EGFR mutation") and negations more than three
  tokens or a clause away from the key term. Protocol `keywords` let authors anchor such criteria
  ("aged", "adolescent"), but an affirmative inclusion condition still needs a scoped negation,
  except that a study design criterion's keywords name other designs. Other interventions are
  not read that way: "received varenicline" against "One arm receives NRT" stays `unclear`,
  because the other arm may have had NRT (the gold set's genetic study is such a case). A
  wording with a generic noun ("Randomized controlled study") verifies no other design, since
  "a cohort study" names its word "study"; nor does a record that mentions trials or
  randomization in passing. The whole-sentence reading of negations also turns base cases
  unclear: "patients without prior chemotherapy, aged 18 years or older, with ..." names an
  age, and "Patients without prior chemotherapy were enrolled" has words after the negated
  term (German verb-final sentences mostly do).
  Measure the work saved this costs on gold data before loosening any rule.
- Accepted limitations of the filter, not to be chased with more patterns: foreign content words
  that carry negation ("Melanoma assente"); a negation in an intervening clause about another
  condition within the window ("Patients with no diabetes received chemotherapy"); evidence about
  another arm or population; partial matches ("skeletal metastases" against "brain metastases");
  and in general anything semantic, since the filter is not a semantic verifier. Mitigation: every
  record is seen by a human, an AI exclusion is a proposal, and disagreements, AI-only exclusions,
  and audits go to adjudication. Requiring all wording terms (up to three) in the quote would
  narrow partial matches, but it rejects the positive controls ("Patients without prior
  chemotherapy" against "Previously treated with chemotherapy"), so it is not done.
- Polarity is read in English and German only (`judge.verify.POLARITY_LANGUAGES`). A not_met
  on a criterion without an en or de wording, or with a quote that is not positively English or
  German, becomes `unclear` (`language_unverifiable`). Add a language by adding its function
  words and negation cues.
- A live test against Ollama or vLLM; the tests use recorded response bodies.

## WP-09 Screening

Module `kittum.screening`. Needs: `protocol`, `corpus`, `judge`, `ledger`, `runs`, `stats`.

- Assignments per mode (M1 to M3), blinding enforced in queries, reveal-after-commit with
  provisional and final decisions, sentinels and audit samples hidden from reviewers, conflicts,
  adjudication.
- Acceptance: a reviewer can never read the other reviewer's decision or the AI verdict before
  committing; tests prove it at the service boundary.

**Done.** `Screening` (migration `20261029010000_screening_tables.sql`); every write is a
`Commands.execute` command with a pure decider, a receipt, and ledger events.

- `open_stage(spec)` opens one scope at one stage of a published protocol version. The topic's
  mode fixes the screeners (two in M1 and M3, one in M2). Every stage names an auditor, who is not
  a screener, and its adjudicators. `sentinel_percent` of the record count (rounded up) is drawn
  from the given sentinels and given to every reviewer; `random_audit_percent` of the records also
  go to the auditor. Both draws are seeded and named in `screening.stage_opened`. M1 and M2 stages
  need a nonzero audit percentage (see the extra screens below). Criteria that apply to the
  whole topic apply to each of its scopes (`criteria_for`).
- `request_judging` enqueues `screening.judge` jobs in batches (stage keys per stage, judge
  config, and batch, so asking again adds nothing); `judge_handler(db, screening, runner)` runs
  them in a `Worker`. The runner is `functools.partial(judge.judge_many, ...)`, built by the entry
  point because screening may not import `models` or `storage`; verdicts become the stage's AI
  readings through a command whose ID derives from the job (`screening.ai_read`). An item enters
  queues once the AI has read it.
- Queues: M1 and M2 follow a seeded order per reviewer; M3 follows the AI's order (proposed
  exclusions last, then by criteria not met), shared by both screeners. `queue` returns batches
  after a cursor (`after`), as the workbench prefetches five.
- Two readings of each verdict. `ai_excludes` is `Verdict.excludes` (verified by the filter); it
  counts where an AI exclusion would remove or deprioritize a record: the M2 disagreement when the
  human includes, the M3 order, and the audit strata. The model's own reading includes a record
  when no criterion's model-reported status (`downgraded_from` if set, else `status`) is
  `not_met`; it counts where the AI argues for keeping a record that humans excluded: the M1
  AI-include rule and the M2 disagreement when the human excludes. A `not_met` that only the
  filter downgraded is therefore agreement with a human exclusion, and no exclusion the filter
  could not verify is ever revealed against an inclusion. A record that tried to instruct the
  model has no trustworthy own reading and counts as included. (Integrator's decision, after the
  first review: the filter stops wrongful exclusions; it must not send nearly every record both
  humans exclude to adjudication.)
- `decide`: a provisional decision, then a reveal if the stage's `reveal` setting and the AI say
  so (on disagreement as above: the AI's verified reason to exclude, or its reading of the human's
  criterion), then a final decision. Both are stored with the client's timing (shown, first key,
  decided, reveal shown). Clients see an opaque `etag` per item and conflict, random per state,
  never a revision count; they send it back as `expected_etag` (`StaleRevision` if it is old).
  Revisions stay internal and in the ledger. A replayed command returns the stored result.
  Exclusions need a criterion ID. `undo` returns the item to the queue under a new token, keeps
  the old decisions, and withdraws an open conflict; an adjudicated item cannot be undone.
- AI-only disagreements never go to adjudication; they become blind extra screens (integrator's
  decision after the third review, replacing an earlier design with decoys in the adjudication
  queue, which could not be made tight). When M1's screeners both exclude while the model saw no
  failing criterion, or M2's lone human and the AI disagree (provisionally or finally, the
  protocol's census), the record goes to the stage's auditor: a new auditor assignment, seeded into
  the auditor's queue among the random audit samples and sentinels and alike in every field, or
  the auditor's existing one if the record is an audit sample. The ledger records each routing as
  `screening.ai_disagreement_rescreen` (item, reason, auditor) for the AI disclosure. The auditor
  screens it like any record, with the normal reveal rules for their own decision. If they agree
  with the screeners, the human decision stands; if they disagree, that human disagreement goes to
  adjudication.
- Conflicts are human disagreements only: the screeners disagree on inclusion (or at full text
  exclude on different criteria), or the auditor's screen disagrees with them. Membership in the
  adjudication queue therefore says nothing about the AI, and the AI column that opens after the
  adjudicator commits leaks nothing about other items. `adjudication_queue` shows each human's
  final decision under a pseudonym (A, B, C), never to someone who reviewed the record, in an order
  hashed from random conflict IDs; only the stage's adjudicators may adjudicate. `adjudicate`
  adopts a column or takes an own decision. `outcomes` gives each record's current outcome
  (include, exclude, conflict, or not yet screened, which includes waiting for an extra screen)
  for the lead and later stages.
- `status`: screeners and the auditor see only their own counts; adjudicators the screening
  progress and their pending adjudications; anyone else (the methods lead) the stage: conflicts,
  the extra screens and their outcomes (`rescreens`: triggered, pending, upheld, adjudicated),
  and the metrics below.
- Blinding below the service: reviewer reads go through the SQL functions
  `screening_reviewer_queue` and `screening_reviewer_progress`, which return only the reviewer's
  own assignments in one stage, no AI output (except their own reveal while it awaits the final
  decision), no sentinel or audit flags. Two topics that screen the same scope keep separate
  queues and counts; a scope in several topics needs `topic_id`. The lead's view has kappa and
  PABAK (human pairs; human-AI against the
  model's own reading as `human_ai` and against the verified reading as `human_ai_verified`; all
  on blind provisional decisions), reveals shown and followed, the audit gauge (sentinel accuracy,
  `stats.elusion_estimate` over the records excluded without adjudication, in two strata by
  `ai_excludes`), and the M3 stop test (`stats.stop_test` over the fully screened
  prefix of the AI order, with the minimum screened share). An open conflict in the tested prefix
  counts as relevant in the p-value but withholds certification: the verdict is
  `pending_conflicts`, with their number, until they are adjudicated.
- Full text: include, exclude, or insufficient information, with optional per-criterion readings
  (met, not met, unclear) and evidence anchor IDs (opaque UUIDs until WP-10). One reason decides,
  as PRISMA 2020 asks: an exclusion cites the criterion it fails, read not met with evidence; an
  inclusion says every criterion is met, so no reading may say otherwise, and it needs no evidence;
  insufficient information names an unclear criterion and cites at least one anchor. (Round 3,
  after Max's review of the demo: a verdict on every criterion is not required.)
- Reveals say why. `Reveal` and the adjudicator's `AiColumn` carry `quote_verified`,
  `downgraded_from`, and where the verified quote is (`QuoteSpan`: title or text, code points,
  found by `find_quote`). An unverified quote is never placed. The full-text route returns the
  revealed quote as an unsaved anchor (`ai_quote`) once the reviewer's own provisional decision
  revealed it (`Screening.own_reveal`). The adjudicator of a full-text conflict reads its PDF
  (`GET .../adjudications/{id}/full-text`, `Screening.conflict_source`) and adopts evidence of
  their own; the AI's verified quotes come on the page (`ai_quotes`) only after they adjudicated.
- Wrong document (round 3, refs #60). `withdraw_item` takes the record of a reviewer's own,
  undecided full-text assignment out of every full-text stage of the project that holds it, one
  `screening.item_withdrawn` per stage (migration `20261031010000_screening_withdrawn_items.sql`):
  it leaves every queue, count, outcome, agreement figure, and audit stratum. It is not an
  exclusion. It is refused once anyone has decided the record in any of those stages, even
  provisionally, so a reveal cannot be escaped and nobody's decision is undone. A lead-only way to
  replace a document after decisions is left open. `kittum.app.mark_wrong_document` does it with the detach of the PDF from the record
  (`project.full_text_detached` with who, when, and the note; `full_text_detachment`), so the
  record has no full text again and the report's PRISMA flow counts it as not retrieved as of the
  event, under the project's command lock, so a retry waits for the first request and returns its
  result. Left open: a record whose real full text arrives later cannot join a full-text stage that
  is already open, the same as any late full text.
- Routes in `api/routes/screening.py`: `GET .../screening/queue`, `POST .../assignments/{id}/decide`,
  `POST .../assignments/{id}/undo`, `POST .../assignments/{id}/wrong-document`,
  `GET .../adjudications`, `POST .../adjudications/{id}/decide`,
  `GET .../status`. The caller's actor ID comes from the `X-Kittum-Reviewer` header until WP-15
  adds authentication; it is a stand-in, not a security boundary.
- `tests/screening/test_rescreen.py` covers the extra screens in M1 and M2, upheld and disputed;
  `test_escalation.py` the two readings (a downgraded `not_met` with both M1 humans excluding is
  not escalated and stays in the audit frame; a model that saw every criterion met gets an extra
  screen; the same in M2); `test_topics.py` and
  `test_m3_stop_waits_for_conflicts_in_the_tested_prefix` the first review's findings.
- Tests (`tests/screening/`) run the nicotine replacement example, the recorded PubMed search and
  a CENTRAL export, the corpus, and the judge on `RecordedBackend`, against Postgres, through the
  worker and the API.

Left open:

- The protocol has no `reveal` field; it is set per stage when the stage opens and recorded in the
  ledger. If it belongs in the protocol (WP-03), read it from there.
- Screening records come in as `ScreeningRecord`s: `corpus` exposes no read of an identity
  revision's reports, and screening may not import `search`. An entry point (CLI or route) that
  builds them from the corpus, and one that opens stages and runs a judge worker, are left to
  WP-15 and the `kittum worker` entry point of WP-02.
- `judge_many` holds the job's transaction across model calls (WP-08's open item). The handler
  records verdicts in a transaction of its own, so a rerun calls no model, but long batches hold a
  connection.
- The audit sample is drawn up front from every record, so the share of the excluded population
  it covers is close to, not exactly, `random_audit_percent`. A found miss goes to adjudication;
  the queue of the 20 nearest AI-excluded records (DESIGN.md) is not built.
- M3 reports the stop test but does not stop the queues; stopping is the methods lead's decision.
- One judge per stage. A second model family as an independent judge, and capture-recapture over
  the provisional decisions, are not wired.
- Changing the mode or the reveal setting of an open stage, and new scopes on a screened corpus,
  need a new stage; screening does not carry decisions across protocol versions yet.
- Reviewer metrics for themselves after a session, the time-to-first-key summaries, the
  disagreement rate per criterion, and break suggestions after a missed sentinel are not computed;
  the timings they need are stored.
- The example protocols' M1 topics sample no records for audit, so their stages are refused
  until the protocol sets `random_audit_percent` (the tests set 10%). The examples belong to
  WP-03.
- The auditor's queue grows when an extra screen arrives, and the share of extra screens among
  the audit samples sets how much an item there hints that the AI disagreed. A larger audit
  percentage dilutes it; the rate is not tuned against the expected number of disagreements.
- The judge's filter confirms few exclusions on the example protocols (wordings with "without", or
  intervention keywords without a negation, fail the polarity check), so the verified reading
  mostly includes.
  Escalation uses the model's own reading, so this no longer floods M1 adjudication, but M3 order
  and the audit strata see few verified exclusions.

## WP-10 Documents

Module `kittum.documents`. Needs: `gateway`, `ledger`.

- Page triage with pdf-inspector; parsers for PyMuPDF, pypdfium2, and Docling behind `Parser`;
  OCRmyPDF for scans; NFC canonical text with word boxes in PDF points.
- Layer storage by content hash; `resolve` with exact, candidate, ambiguous, and unresolved.
- The full-text waterfall (docs/sources.md).
- Acceptance: fixture PDFs with multi-column text and a merged-header table; anchors survive a
  re-parse with a different engine.

Done:

- `triage`: pdf-inspector's page classification and garbled-text flag, plus a PyMuPDF count of
  characters a working ToUnicode map never yields. Each page is `text`, `ocr`, or
  `broken_encoding`, and the layer keeps the verdict.
- `PyMuPDFParser` (the base layer), `PdfiumParser` (differential checks, `compare_layers`), and
  `DoclingParser` (the `docling` extra: Heron layout order and TableFormer grids, every character
  from PyMuPDF). All share one assembler: NFC text, ligatures expanded, line-end soft hyphens kept as
  "-" so `find_quote` joins them, multi-column reading order, running headers first, tables as
  rows of cells. Boxes are unrotated top-left PDF points; the reading frame follows the text
  direction, so sideways landscape pages and pages turned by /Rotate both read in order.
- Tables without PyMuPDF's Layout model (`find_tables(use_layout=False)`, per
  docs/document-tools.md): ruled tables from `find_tables`, three-rule tables from their "Table N"
  caption and full-width rules. The grid comes from the words: rows are printed lines, columns are
  gaps most rows leave empty, header phrases crossing a gap or underlined by a rule span columns,
  and empty header cells merge upwards. Cells carry spans and header paths.
- `OcrMyPdfParser` OCRs the pages triage flags. It needs `ocrmypdf` and `tesseract`; without them
  `ocr_available()` is False and the parser refuses to start. Not exercised here (not installed).
- Storage (migration `20261028025000_documents_layers.sql`): `document` by SHA-256, `acquisition`,
  immutable `layer` (unique per document and recipe hash, JSON in the artifact store, output hash
  = artifact hash) and `anchor`, `anchor_mapping`, and `page_render`. `derive(tx, document, parser,
  artifacts=...)` is idempotent per recipe. `ingest` refuses truncated PDFs.
- `make_anchor(layer, span)`: quote with 32 characters of context, one box per printed line (partial
  words cut along their reading direction, so sideways words too), and cell refs.
  `resolve(anchor, layer)` gives `exact` (same place, unchanged context), `candidate` (unique match
  elsewhere, changed context, or a fuzzy respelling whose numbers are unchanged), `ambiguous` (with
  every match as an alternative), or `unresolved`; repeated values resolve by region. Matches must
  start and end on token boundaries, so "12" never matches inside "123" or "12.5". `resolve` and
  `remap` raise `DifferentDocument` for a layer of another document: a revised PDF is a different
  document and needs a document-revision mapping, which never carries acceptance across. `remap`
  records the mapping once per anchor, layer, and method version (now `resolve.v2`).
- Viewer routes in `api/routes/documents.py`: upload, document with pages and triage, page PNG at a
  scale (unrotated, cached in the artifact store), word boxes per page, create and get anchors.
- `acquire` walks a list of `FullTextLocation`s through the gateway, keeps the first readable PDF,
  stores source, licence, and terms URL, and writes `document.acquired` or
  `document.acquisition_failed` with every attempt.
- Fake: `kittum.documents.testing.TextParser` builds layers from plain text, no PDF needed.
- Fixtures: two CC BY 4.0 BMC Medicine RCTs (tests/documents/fixtures/ATTRIBUTION.md).

Open:

- Source resolvers that turn a record into `FullTextLocation`s (Europe PMC, Unpaywall, OpenAlex,
  publisher TDM) and the human upload queue; `acquire` takes the list.
- The upload route stores the document and its base layer but no acquisition: it has no project
  or actor yet (needs API auth and the Postgres ledger store).
- Ask Artifex where the PyMuPDF Layout source is. With it, `find_tables` could gate borderless
  tables itself; until then the caption-and-rules finder covers three-rule journal tables only.
- OCR on a machine with Tesseract; RapidOCR; PaddleOCR-VL adapters; GROBID metadata.
- `core.find_quote` either joins every hyphenated line break or none, so a quote that crosses a
  split word ("resid-" / "ing") and a compound broken at its hyphen ("middle-" / "income") in the
  same passage does not match. A change in `core`, not here.
- Docling runs per document (about 30 s per page on a CPU here); run it as a job, on pages with
  tables first.

## WP-11 Extraction

Module `kittum.extraction`. Needs: `documents`, `protocol`, `models`, `runs`.

- The extraction schema lives in `kittum.extraction` (`load_schema`, YAML or JSON): arms,
  comparisons, endpoints with timepoints and quantities (N randomized and analysed, events,
  percentages, means, effect estimates, CI bounds, p-values, follow-up, descriptions), each with a
  `critical`, `standard`, or `descriptive` policy and optional Janus locator hints.
  `check_against_outcomes` ties outcome endpoints to the protocol's outcome IDs; the protocol
  itself is unchanged. Done.
- The Janus path: a layer as Janus `DocumentEvidence` (lines as text elements, tables with
  folded header paths and group-row sections), one Janus specification per schema, Janus's
  locators and statuses, Janus's typed parser after normalizing minus signs and inline estimator
  labels, and every value anchored on its layer cell with `make_anchor`. Done.
- The LLM path: a PydanticAI agent on any `LlmBackend` (`FunctionModel`, one JSON
  Schema-constrained call per turn), read-only tools (search, read a page, open a table), a call
  and a time budget, and a verifier that anchors each quote with `quote_spans` and checks the
  value is printed in it. Document text is fenced as data. Done.
- Validators on both paths: exact values with units, events at most N, arms sum to the total, the
  CI brackets the estimate, percentages recompute, ratios are symmetric on the log scale. Done.
- Reconciliation per semantic key (agree, disagree, one side only) with the policy's gate,
  second-extractor flag, and the 10% re-check draw. Done.
- Runs are jobs (`request_run`, `run_handler`), published with the job through
  `Commands.execute`. `accept`, edit-and-accept with a source span, and `group_accept` write one
  `extraction.value_accepted` event each; `assertions` is the accepted set. A bundle exporter adds
  the tables and their anchors. HTTP routes in `api/routes/extraction.py`, with the run list
  (`list_runs`), a schema reader, and the models the LLM path can use, for the workbench. Done.
- Acceptance: the Lesotho and Vitamin C trial PDFs with a real schema; Janus anchors checked by
  reading the PDF under the box; agent answers recorded from `qwen3.5:4b` on Ollama
  (`tests/extraction/record_fixtures.py`); planted validator errors; a real disagreement (the
  agent read 51 for the arm Table 1 prints as 52). Done.
- Open: grounding exceptions (a value read from a figure); the second, independent extraction a
  critical disagreement requires; the re-check queue itself (only the draw is recorded);
  re-anchoring accepted values when a project promotes a new parser.

## WP-12 Appraisal

Module `kittum.appraisal`. Needs: `documents`, `extraction`.

- Instruments as data (RoB 2, ROBINS-I V2, plus profile-registered forms); quote assembly per
  question.

## WP-13 Statistics

Module `kittum.stats`. Needs: nothing.

- Every function in the module, with scipy where it helps. Done.
- `clopper_pearson` defaults to one-sided bounds, as Kittum states every bound: 10 of 10 gives
  a lower bound of 0.741, and the two-sided 0.692 figure remains available with `sided="two"`.
- `elusion_estimate` is a stratified Horvitz-Thompson total with simultaneous one-sided exact
  hypergeometric bounds (Bonferroni over strata, exact on census strata). Sampling is without
  replacement, so the bounds invert the hypergeometric tail rather than a binomial one.
- `stop_test` implements Callaghan and Müller-Hansen (2020) with the suffix-minimum p-value from
  the reference implementation. Pass `order` for the exact reference result; without it the
  test uses the whole sequence and is conservative.
- `capture_recapture` uses Chapman for two sources with an exact hypergeometric overlap
  inversion (the upper end is unbounded when the overlap is zero), and a Poisson log-linear
  model without a three-way interaction for three sources. Zero observed cells make that model
  non-estimable and raise a `ValueError` naming the cell.
- `conformal_threshold` is a split-conformal order statistic with a binomial coverage guarantee;
  `recall_needed` gives its calibration size for zero misses.
- `cohen_kappa`, `fleiss_kappa`, and `pabak` cover two raters and three or more runs.
- Acceptance: tests against published worked examples, including 10 of 10 giving a 0.741
  one-sided (0.692 two-sided) lower bound, the 119-record case from DESIGN.md, the buscarpy
  stopping reference, and seeded simulations for every interval. Done.
- Open: the evaluation harness (WP-17) still has to pool these bounds across topics.

## WP-14 Reports

Module `kittum.report`. Needs: most modules.

- The core targets: PRISMA 2020 flow per scope, RIS exports, the handover bundle, and the AI
  disclosure. Profile targets register through `report.register_target`; the `gba_amnog`
  targets are listed in docs/profiles/gba-amnog.md.
- Acceptance: golden files for a small fixture project.

**Done.** Every target renders from `load_review(tx, project_id, cutoff_seq=...)`: the review
as the ledger recorded it through a cutoff (default: the head). Screening is replayed from the
append-only decision, undo, and adjudication rows, and conflicts and extra screens from their
ledger events (`screening.conflict_*`, `screening.adjudicated`,
`screening.ai_disagreement_rescreen`), so a report at an old cutoff shows the review as it stood
then. A record sent to the auditor's blind extra screen stays unresolved until the auditor
decides. Migration `20261029020000_report_artifacts.sql`.

- `prisma`: `flow(review, topic, scope)` from one frozen identity revision (default: the latest
  at the cutoff). Identification counts the occurrences of the topic's searches that revision
  holds; records of searches run after it are reported apart (`records_outside_identity`). Each
  topic's flow is self-contained, as PRISMA reports one per question: a report several topics
  found is identified and unique in each. So it equals `corpus.get_counts` (the whole revision)
  only when one topic was searched; with several, the topics' identified records sum to its
  total and the union of their unique records is its reports. Records per source and per kind (databases, registers, other),
  duplicates, screened and awaiting screening (screened as screening counts it: every screener
  decided, and the auditor of an extra screen), excluded, sought, not retrieved (sought but not
  in the full-text stage), assessed and awaiting assessment,
  exclusions per criterion ID, awaiting classification (insufficient information), included
  reports and studies. `members` lists the reports behind every count; `checks` states every sum
  that must hold and reports a broken one. The PRISMA automation box is always 0 (the AI never
  removes a record); exclusions with an AI proposal, after a reveal, or after an extra screen the
  AI's disagreement triggered are counted separately. JSON and an SVG diagram per scope.
- `ris`: one undeduplicated file per search, the deduplicated corpus (one entry per report), and
  per scope the included, title-and-abstract excluded, and full-text excluded lists. `AN` is the
  provider ID, `N1` the registry IDs and decision, `C1`/`C2`/`C3` the occurrence, report, and
  study. Fields are written on one line (RIS cannot carry line breaks); the bundle keeps the text.
- `search_report`: every search with platform, dates, the strategy as run, limits, study filter,
  the query translation, reported and retrieved counts, raw hashes, and who ran it; planned
  sources without a search and source deviations.
- `ai_disclosure`: the disclosure record (`disclosure(review)`: recipes, weights hashes and
  licences, the identities the server reported, prompt and schema hashes, decoding, mode per
  topic with rationale and validation pack, reveal per stage, kappa and PABAK against the model's
  own and the verified reading, reveals, adjudicated conflicts, the audit gauge, the extra screens
  the AI triggered with each one's reason and result (upheld, adjudicated, pending),
  `judge.VERIFIER_LIMITS`) and one RAISE-aligned Markdown rendering. Agreement carries its 2x2
  table (`PairAgreement.include_include` and the other cells) and reads as "agreed on N of M
  records". `prompts` holds one card per AI role the review used (judge per stage and scope,
  assistant per purpose, the LLM extraction path), read from the first stored request by
  `report.model_calls.prompt_cards`: system prompt, user prompt template (the judge's) and the
  first user prompt as sent, JSON schema, request settings, and the recorded recipe.
- `extraction`: the accepted values at the cutoff, the latest per semantic key
  (`report.extracted`), as `extracted_values.csv` and `extracted_values.json`: study, report,
  field, value, unit, the path that produced it (`janus`, `llm`, or `manual_edit`), pages and
  quote, who accepted it, and when. `kittum verify` checks both files against the ledger's
  `extraction.value_accepted` events through `ledger.register_bundle_check`.
- The audit trail (`report.audit`): each traced event says who did what in words, names the
  criterion's wording, quotes, and files, and recomputes its payload and event hashes; an AI step
  lists its model calls (`report.model_calls.calls_of`) with the stored request and response;
  `GET .../audit/artifacts/{sha256}/check` hashes an artifact again.
- `render(tx, target, project_id, scope_id, store=...)` stores the files by hash and returns an
  `Artifact`; `record_render` writes `report_artifact` and a `report.rendered` event.
- `seal_bundle(tx, project_id, store, out_dir, signer=..., actor_id=..., rights=...)` renders
  every core target into `reports/<target>/` and the protocol's profile targets into
  `reports/dossier/<name>/`, at the bundle's cutoff, and seals with every registered exporter.
  `kittum.report` registers exporters for the protocol, search, corpus, judge, screening, and
  report tables (`report/exporters.py`), each filtered to rows whose event is at or before the
  cutoff; mutable columns are left out or recomputed. A `RightsProfile` withholds raw files of
  named sources or hashes; a referenced artifact missing from the store is listed by hash with
  that reason.
- `gba_amnog` targets in `kittum.profiles.gba_amnog.reports`, registered by `load_profiles`:
  `anhang_4a`, `anhang_4b`, `anhang_4c`, `anhang_4d`, `justification_4235`, `anlage_ii_1`
  (Markdown and CSV, RIS for the tree).
- Routes in `api/routes/report.py`: `GET .../reports/targets`, `POST .../reports/targets/{target}/render`,
  `GET .../reports/artifacts/{sha256}` (only files rendered for the project), `POST .../reports/bundle`.
  Rendering, downloading, and sealing are for the methods lead only (`report.require_lead`): a
  human who screens, audits, or adjudicates no stage of the project, the rule `Screening.status`
  uses for the lead's view. Everyone else gets 403.
- Changes outside the module: `BundleWriter.file`, `BundleWriter.read`, and
  `register_bundle_check` in `ledger/bundle.py` (exporters add files the manifest lists and
  checks of them); `PairAgreement.of` and its 2x2 cells in `screening`; `judge.USER_TEMPLATE`;
  `kittum verify` imports `kittum.report` so its checks run; `judge.VERIFIER_LIMITS`; `ALLOWED` lets `report` import `judge` and
  `profiles` import `report`.
- Tests (`tests/report/`, `tests/profiles/test_gba_amnog_reports.py`) build the nicotine
  replacement review and the capivasertib dossier through search, corpus, judge, and screening
  against Postgres, and check the PRISMA sums by hand and from data, earlier cutoffs, the RIS
  round trip, the PRISMA-S fields, the disclosure, the sealed bundle through `kittum verify`, the
  trace from report numbers to decisions, rescreen events, and people in the bundle's SQLite, the
  routes, and each copied metric against `Screening.status`.

Left open:

- The PRISMA target has no update-search delta and no methods text yet (DESIGN.md "Reports").
- The Anlage II.1 file names are provisional (`ANLAGE_II_1`, and `GAPS.md` in the output); the
  profile doc does not record them. The tree has no PDFs until WP-10. `gba_amnog.modul4` (the
  Modul 4 text and tables, Anhang 4-E and 4-F) and `gba_amnog.jca` wait for extraction and
  appraisal.
- Anhang 4-A shows the strategy line by line, with the hit count only on the last line: searches
  do not record per-line counts.
- `kittum verify` does not yet re-run the count reconciliation; the `checks` in each flow do it at
  render time. `registry/` (lockfiles, SBOM, parser recipes) is not in the bundle; model recipes
  and prompt hashes are, in `judge_config`.
- The exporters live in `report` and read the other modules' tables directly; an owner may move
  its exporter into its own package. Enrichment rows are not exported.
- A proposal's status is not replayed to the cutoff (it is left out of the bundle); the
  confirmations are ledger events.
- Screened counts, open and resolved conflicts, agreement, reveals, the audit gauge, and the
  rescreen gauge are recomputed from the replayed rows with `screening.metrics`'s definitions, so
  they stay exact at old cutoffs. `test_a_metric_the_report_computes_equals_what_screening_reports`
  compares each with `Screening.status` on both reviews, so a change there fails here by name.
- The report routes take the actor in the body or the `X-Kittum-Reviewer` header until WP-15 adds
  authentication; the lead check is only as good as that stand-in.

## WP-15 API

Modules `kittum.api`, `kittum.app`, and `kittum.people`. Needs: the modules it exposes.

- Routes for the workbench screens, thin over module services. Regenerate contracts after each
  change.

**Done** (branch `kittum/wp-15-integration`, on main with WP-09, WP-11, and WP-14):

- `kittum.app` is the composition root, an entry point like `api` and `cli` (`"app": {"*"}` in
  the architecture test). `AppContext` moved there; `kittum.api` re-exports it.
  `AppContext.from_settings` builds the Postgres ledger, the job queue and command receipts
  (extraction's commands use them), the filesystem artifact store, the `SourceGateway` (default
  policies, `HttpxTransport` with the contact email), every model backend in
  `Settings.llm_endpoints` by recipe (with the deployment's own recipes from
  `KITTUM_RECIPES_FILE`), and the screening `Judge`, which may be a hosted recipe.
- Settings: `KITTUM_LLM_API_KEYS`, `KITTUM_RECIPES_FILE`, `KITTUM_JUDGE_MODEL`,
  `KITTUM_JUDGE_BACKEND` (`endpoint`, or `demo` to replay the demo's recorded answers),
  `KITTUM_WEB_DIST`; all listed in `kittum/.env.example`.
- `kittum serve` serves the API and, with `KITTUM_WEB_DIST` or `--web-dist`, the built workbench
  (files under `/assets` immutable, `index.html` for every other non-API path).
  `kittum worker` runs a `Worker` with every registered job kind (`register_job_handler(kind,
  factory)`): the screening judge, and extraction's `run_handler`. Without a model server an
  extraction run takes the Janus path; one that asks for the agent gets LLM proposals recorded
  as errors that name the missing backend. A judge worker
  refuses, before any model call, the jobs of a stage that names another judge config.
- `kittum.people` owns who works on a project and in which roles (lead, reviewer, auditor,
  adjudicator; migration `20261029040000_app_projects.sql`, table `project_member`); screening
  and report may import it (the `ALLOWED` table). `kittum.screening.sees_lead_data` is the one
  rule for what only the lead sees: a project lead who screens, audits, or adjudicates no stage.
  `Screening.status`'s whole-stage view (anyone else outside the stage gets 404),
  `report.require_lead`, the catalogue routes below (403), and progress's identification counts
  and outcomes all use it, so a lead who also screens is a screener first.
- The project lifecycle (`kittum.app.lifecycle`), each step a function on the caller's
  transaction with a ledger event, a route, and a `kittum project` command: create a project and
  its lead, add people with roles, publish a protocol, run or import
  searches (the search routes), ingest the corpus (once; update searches need the corpus diff),
  attach full texts (table `full_text`, event `project.full_text_attached`), open a stage, and
  progress. A stage takes its screeners, auditor, and adjudicators from the people; title and
  abstract screens every report of the newest identity revision; full text every record included
  at title and abstract that has a full text, with sentinels from the project's sentinel full
  texts (`attach_sentinel_full_text`). Opening a stage enqueues the judge. `kittum project
  open-stage --sentinels FILE` takes title and abstract sentinels from a YAML or JSON list
  (`examples/sentinels/nicotine-replacement.yaml`, which the demo uses too); a refused step
  prints why and exits 1.
- `kittum demo seed [--live] [--no-wait] [--json]`: the nicotine replacement example (no
  profile), a PubMed search replayed from a recording (`scripts/record_demo_search.py`, 20
  records), an example CENTRAL export with one duplicate of a PubMed record (confirmed by the lead
  in the seed), a citation search with the two CC BY trials and a registry entry that shares the
  Lesotho trial's NCT number (a pending study link for the Studies screen), both PDFs as full
  texts, the Vitamin C PDF as a full-text sentinel, five people, and an open title and abstract
  stage. Recorded answers (`demo_data/judge_answers.json`) are what `qwen3.5:4b` on Ollama
  answered (`scripts/record_demo_answers.py`).
- Server additions for the workbench, each in its module's route file:
  - `projects.py`: `GET/POST /api/projects`, `GET /api/projects/{id}`, `POST .../members`,
    `POST .../full-texts/{item_ref}` (PDF body), `GET .../progress` (each stage the viewer has a
    part in, as they may see it; identification counts and outcomes for the lead only).
  - `ledger.py`: `GET .../ledger/verify` (the report and event counts by type; events themselves
    are not served, since their payloads hold every reviewer's decisions).
  - `screening.py`: `GET/POST .../screening/stages`, `GET .../assignments/{id}/full-text` (the
    document, layer, and protocol-term mentions per criterion, from `kittum.app.fulltext`). Rule
    violations (422) now use FastAPI's validation-error shape, so the generated client decodes
    them.
  - `corpus.py`: `POST .../corpus/ingest`, `GET .../corpus/revisions/latest`. That, the link
    proposals, and `search.py`'s executions take `X-Kittum-Reviewer` and answer 403 to anyone
    but a project lead: a reviewer who could list every record could tell the sentinels in their
    queue by their absence.
  - `protocol.py`: `GET .../protocol` (the published version with the profile's labels for core
    concepts, the core's words otherwise).
  - `documents.py`: `POST /api/layers/{id}/anchors` takes `start` to anchor a mention or a
    selection at its offset.
- Module additions the wiring needed: `corpus.latest_revision` and `corpus.reports` (the read
  WP-09 asked for; `CorpusReport` carries the source records); `Screening.stages`, `.stage`, and
  `.assignment_source` (the item reference stays server-side, since a sentinel's names it);
  `documents.derive` restores a layer whose JSON the artifact store lacks (from the parsed layer
  this process still holds, or by parsing again), and refuses if the output hash differs (a
  database paired with another store used to fail on read).
- Tests: `tests/app/test_lifecycle.py` drives a project from nothing to an open stage over HTTP,
  with the judge-mismatch refusal, and a third reviewer outside the stage who gets no catalogue,
  no stage, and no reports, and a lead who also audits and gets none of the lead's data either;
  `tests/app/test_worker.py` runs extraction jobs with no model server;
  `tests/app/test_cli.py` sets a review up from the command line; the
  end-to-end journey (WP-16) covers the rest on the real stack.
- Every lead write is checked inside its transaction against the claimed actor: adding people and
  publishing need a project lead; searches, the corpus and its links, opening a stage, and full
  texts need a lead who screens nothing, as the catalogue reads do
  (`tests/app/test_setup_routes.py` refuses a reviewer's own ID on each).

Enrichment and open-access retrieval (branch `kittum/enrichment-ui`):

- `kittum.app.enrichment`: building the corpus (route and `kittum project ingest`) queues an
  `enrich.revision` job; `kittum project enrich` and `POST .../enrichment/runs` queue another (an
  unfinished run comes back instead; requests for one project take turns under a transaction
  advisory lock, so two at once queue one run). The job asks the sources for 50 records at a time
  with no transaction open (`enrich.lookup`), then, holding the job's lease (`JobQueue.fence`) and
  the run's row lock, writes the chunk's `record_enrichment` rows for records no other worker
  wrote, an `enrichment_run_record` row per record (the sources asked, answering, and failed for
  that record with the reason, from `RecordEnrichment.failures`), and an
  `enrichment.records_enriched` event naming every field's source. `enrichment.requested` and
  `enrichment.finished` frame the run.
- `kittum.enrich` refuses an answer that names another work (DOI, PMID, PMCID) as a failure of
  that source for that record, and uses none of it. A DOI is reduced to one canonical form
  (`kittum.core.canonical_doi`: no `https://doi.org/`, `http://dx.doi.org/`, `doi.org/`, or `doi:`,
  no surrounding space, case-folded) before every lookup and every comparison, by every source and
  by corpus dedupe, so dressed and bare forms of one DOI are one work. A Crossref PDF link needs a
  Creative Commons licence for exactly its content version; the licence's start date travels with
  the location, and `full_text_candidates(..., on=)` skips it until then.
- `kittum.app.open_access`: `POST .../retrieval/open-access` (or `kittum project
  fetch-open-access`) fixes the reports included at title and abstract without a full text and
  queues a `retrieval.open_access` job. Per report it fixes the PDF locations enrichment found
  (`full_text_candidates` on the day), then tries them one at a time: the request goes out with
  no transaction open (`documents.fetch_location`), and the attempt is committed with its ledger
  event (`documents.record_location`: `document.acquisition_attempted`, or `document.acquired`
  with the acquisition) before the next is tried, so a crash or a failed attach keeps the
  evidence and a retried job resumes after the last recorded attempt. Attaching the PDF
  (`attach_acquired_full_text`: parsed layer, licence, the acquisition in the event) is a
  transaction of its own. Every step holds the job's lease and the report's row lock. A report
  without a location gets `retrieval.no_open_access_location`; one uploaded by hand meanwhile
  keeps its upload, and an acquisition made before it stays on record.
- `GET .../ledger/verify` gives everyone whether the chain holds, and how far it was checked only
  while it does (on a broken chain that seq names where it breaks); the full report and the event
  counts by type only to a lead who screens nothing (`X-Kittum-Reviewer`), since the counts of
  acquisition events say how many reports are sought while reviewers screen.
- Each acquisition attempt's event (`document.acquisition_attempted`, `document.acquired`) names
  the report it was for (`report_id`), and the audit trace matches a retrieval run's `report_ids`,
  so a record's audit trail shows its whole retrieval: the request, every attempt, the attach.
- Routes (`api/routes/enrichment.py`), all 403 to anyone but a lead who screens nothing: the
  latest enrichment run with counts per source, flags per record, one record's enrichment (field
  sources, identifiers, notices, OA locations, entity tags, failures), the latest open-access run
  with each report's attempts, and a report's attempts over all runs. Migration
  `20261030010000_app_enrichment_retrieval.sql`.
- `HttpxTransport` speaks HTTP/2: link.springer.com answers HTTP/1.1 clients with a bot challenge.
  `KITTUM_GATEWAY_RECORDINGS` serves the gateway from `kittum.gateway.RecordedTransport`, only
  with `KITTUM_OFFLINE=1` (the process refuses to start otherwise), and then with in-memory rate
  limits and response cache (`SourceGateway(cache="memory")`), so recordings never reach the
  deployment's cache. The end-to-end stack points it at `tests/enrich/fixtures/stack`, recorded by
  `tests/app/record_stack.py` (no key; the CC BY PDFs are the repository's own fixtures).
- Record and replay (README, "Record a live run, replay it"): `KITTUM_GATEWAY_RECORD` wraps the
  live transport in `kittum.gateway.RecordingTransport`, which writes each response, every attempt
  in order, in the `RecordedTransport` format under `recording_key` (URL and query, credential
  parameters dropped, the contact address replaced by `CONTACT_PLACEHOLDER` only in its source's
  `CONTACT_PARAMS` entry, on recording and lookup alike). It refuses (`RecordingRefused`) user info
  or a configured secret or the address anywhere else before sending, and cleans a body that holds
  one, for the live run too. Live only, with a process-local cache. `RecordedTransport` answers a
  recorded list in order and a hand-made single entry every time. `KITTUM_LLM_RECORD` wraps every
  backend in `kittum.models.RecordingBackend` (completion, request, and raw bytes under the request
  hash, in order; refused with `LlmRecordingRefused`, naming the field, when one holds a secret or
  the address), and `build_judge` notes the judge's recipe in the recording's meta.
  `KITTUM_LLM_REPLAY` replaces the backends with `ReplayBackend`, which answers in order and fails
  with `NotReplayed` naming the hash and occurrence, and judges with the noted recipe. A replayed
  completion keeps the model identity and serving and reports its endpoint as
  `replay of <endpoint>`, the one field every verdict, ledger event, and disclosure carries.
  `tests/app/test_record_replay.py` records an enrichment and a hosted judge run and replays it
  offline to the same rows and ledger payloads, and picks the live run's judge;
  `tests/models/test_replay.py` replays a judge run in order, in a file and in a directory, and
  refuses completions holding a key or the address; `tests/gateway/test_recording.py` covers the
  refusals, the placeholder, and a failing-then-answering source replayed in order, a transport
  failure included (recorded as `{"error": <kind>}` and raised again as that kind). Recording
  refuses to start (`check_recording_secrets`) with a key or password under
  `RECORDING_SECRET_LENGTH` characters, a common one, or one inside what Kittum sends (the dev
  password `kittum` is PubMed's `tool`); the database password is compared decoded.
- Tests: `tests/app/test_enrichment.py` drives both jobs through the worker on recorded responses
  with PubTator answering 503, then again with it back (the cache answers the rest), and an attach
  that fails once and resumes; `tests/app/test_enrichment_jobs.py` has a worker lose its lease
  mid-request in each job, two requests race on two connections, a source fail for one DOI, and
  offline mode; `tests/enrich/test_answers_and_licences.py` covers answers for another work and
  licence versions and dates;
  `apps/web/e2e/enrichment.spec.ts` covers the screens, and `setup.spec.ts` checks the enrichment
  of the corpus it builds.

Open:

- Authentication. `X-Kittum-Reviewer` (screening, progress) and the `actor_id` fields in request
  bodies are stand-ins: anyone who can reach the API can act as anyone. The workbench says so.
- Identity revisions after a stage opens (a confirmed link, a merge) do not reach the open stage,
  which keeps the reports it opened with. Update searches need the corpus diff (WP-07).
- One judge per deployment: a worker runs one judge config, and opening a stage uses it. A
  stage with another config needs a worker started with it; a mismatched worker fails the jobs
  for good (they need requeueing by hand).
- Full-text sentinels need a PDF each. The open-access waterfall skips Europe PMC's full-text
  XML: a document is a PDF, and there is no layer from JATS yet.
- Issue #74: the configured contact is redacted from lookup audit URLs and diagnostics before
  the retrieval/enrichment rows and ledger are written. The app gateway scopes HTTP/retry logging
  to its own calls and cleans transport errors and error bodies before persistence or message
  truncation; worker failures and publish callbacks get the same diagnostic protection.
  Outgoing requests and successful source content stay unchanged. Lookup retries and restarted
  workers rebuild their requests from current settings, without expanding audit placeholders.
  Synthetic Postgres tests cover success, empty/malformed replies, redirects, errors, retries,
  lease expiry, worker failures, signed bundles, and unrelated URL/content/log controls.
  This addresses the demonstrated configured-contact lookup/diagnostic paths, not a generic
  scrub/rebind policy for provider-returned file URLs or unconfigured personal `tool` values;
  those remain open. Existing ledger history and signed bundles are not rewritten.
- Writes (confirm, merge, split, render, seal) name their actor in the request body; only
  reports check that it is a lead.
- `kittum serve` runs one uvicorn process; production needs a process manager and TLS in front.

## WP-16 Workbench

`apps/web`. Needs: WP-15.

- Screens from DESIGN.md "Reviewer workbench", starting with the title and abstract queue and its
  keyboard flow. Base UI for components, Legend List for queues, server-rendered PDF pages with
  word-box text layers for full text.

Done (branch `kittum/wp-16-workbench-design`, then `kittum/wp-15-integration`):

- Design system: tokens for light and dark, IBM Plex Sans with its condensed width, keycaps, the
  status vocabulary, and the evidence marker that pairs a selected item with its source.
  `apps/web/README.md` describes it.
- `Workbench` (`src/data/workbench.ts`) has two implementations: `src/data/api.ts` on the
  generated client in `@kittum/contracts` (development and production) and the mock on recorded
  fixtures (unit tests; `VITE_KITTUM_WORKBENCH=mock` for a server-less look). Calls that depend on
  who asks take the reviewer's actor ID, sent as `X-Kittum-Reviewer`. Server records come typed
  from the contracts; `src/data/schema.ts` keeps only what the screens compose.
- Who is at the workbench: a picker of the project's people, remembered per tab (two reviewers
  side by side in two tabs), `?as=` on any URL, and a header chip to switch. It says plainly that
  there is no sign-in. Each person starts where their work is.
- Title and abstract queue on the server: five prefetched, decide, reveal on disagreement after
  the reviewer's own decision, keep or revise, Backspace reopens and a new decision undoes the old
  one on the server, an item awaiting a final decision comes back with its reveal, and a queue
  that waits for the AI judge says so instead of "finished". The auditor screens here too.
- Full text on the server: page PNGs and word boxes from the documents routes, protocol-term
  mentions per criterion, `Enter` saves a mention or a selection as an anchor, the decision
  carries a reading and evidence per criterion, the AI's reading after a provisional decision
  (keep with `Space`), and the next full text.
- Adjudication: the record with the pseudonymized columns side by side, `1`/`2` adopt, `i` `e`
  `u` own decision, the AI column after the commit, `Space` next.
- Progress: the PRISMA-style flow (identification by source, duplicates, per stage screened,
  excluded, in conflict, waiting, included; full texts; studies), and for the lead agreement
  (kappa, PABAK; human pairs and human-AI), reveals, extra screens, sentinels, the audit gauge
  with the elusion bound, and the ledger check; `n` opens full-text screening for a scope.
  Reviewers see their own counts only.
- Protocol (read only, with profile labels), Search (executions, counts, line-numbered
  strategies, the platform's translation), Studies (proposals first; `Enter` confirm, `x` mark,
  `m` merge, `s` split). Search and Studies are the lead's: anyone else sees why, not the records.
- Reports (the lead's): `p` renders the PRISMA 2020 flow from the ledger and shows its counts and
  diagram per scope, `s` seals the handover bundle and says how to verify it offline.
- End to end: `apps/web/e2e/review.spec.ts` (Playwright, `pnpm --dir apps/web e2e`, CI job
  `kittum-e2e`) seeds the demo and starts `kittum serve` and `kittum worker` with the recorded
  judge, then runs one journey: reviewer A screens by keyboard with an undo and a reveal (nothing
  of the AI on screen before A decides); reviewer B screens and every screening response B
  received is checked for A's ID and for AI rationales outside B's own reveals; the auditor gets
  the AI-only disagreements; the adjudicator decides with no AI output on the page and then sees
  it; the lead checks the flow, the excluded and included counts, conflicts, extra screens, and
  kappa, and opens full text; both reviewers and the auditor screen the real Lesotho PDF and the
  full-text sentinel with adopted evidence; the lead confirms the study link; studies drop from 29
  to 28, the full-text extra screen is upheld, and the ledger verifies in the UI and with
  `kittum ledger verify`. Reviewer B and the auditor get 403 from the catalogue and no lead
  figures, and no screening or progress answer they received has an AI verdict field, another
  person's decision, or a reveal outside their own decision. The lead renders the PRISMA flow
  (the same counts as Progress) and seals a bundle that `kittum verify` accepts.
- Setup from the browser (branch `kittum/workbench-setup`): the start page creates a review
  and makes its creator the methods lead; Setup lists the steps in order with what is done from
  the server's records (people, protocol, searches, corpus, title and abstract screening, full
  texts, extraction), adds people with their roles, and opens a stage per scope with its reveal
  setting and a sentinel file, showing the protocol's mode, audit plan, and who will screen.
  Protocol has an editor for the lead: an example protocol, the published version as YAML, or a
  file; `Ctrl+Enter` checks it (the server reads the YAML and lists each problem with its path),
  and the diff against the published version shows before publishing with a reason. Search
  imports export files (RIS, CENTRAL, Embase, PubMed XML, registry CSV) and runs PubMed or
  ClinicalTrials.gov live; Studies builds the corpus with `b`; Retrieval (the lead's) lists every
  report with its title and abstract outcome and uploads each PDF with its licence, and adds
  full-text sentinels. `apps/web/e2e/setup.spec.ts` does all of it in the browser from an empty
  review, runs the same steps through `kittum project`, and checks that both reviews open the same
  stage (mode, reveal, protocol hash, judge, the people in each role) and fill the screeners'
  queues with the same records and sentinel; the first reviewer then picks themselves and screens.
- Extraction on the server (branch `kittum/workbench-control`): the lead requests a run (`n`) on
  a report included at full text (the
  server refuses any other, and a layer of another document), with an
  extraction schema the server reads and the Janus path only or the LLM path on a configured
  model; the list follows the worker until the run is ready. The review shows the server's fields
  grouped by their labels: `Enter` accepts once the source was on screen, `1`/`2` pick a path,
  `e` turns a selection on the page into an edit (the first number in it, to confirm in a field,
  then `Enter`), `o` accepts despite a failed check, `a` accepts the agreeing standard fields
  together, each only once its row was on screen for the floor. The accepted values, the latest
  per semantic key, are listed under the runs. A run, its fields, and its accepted values reach
  only a lead who screens nothing or a member with no undecided screening assignment for the
  report (`kittum.app.visible_reports`); anyone else gets no run and a 404.
  `apps/web/e2e/extraction.spec.ts` screens the Lesotho trial to full-text inclusion in the
  browser (nothing can be extracted before), requests a Janus-only run, shows the auditor whose
  audit screen is open nothing of it, accepts Table 1's 52, and edits the 12-month HbA1c from a
  mouse selection on page 5; the assertion's anchor is that span. `tests/app/test_extraction_access.py`
  checks every extraction read and acceptance against Postgres.
- The Audit screen (branch `kittum/audit-trail`, `g` then `l`), shown only to a lead who screens
  nothing, over lead-only routes under `/api/projects/{id}/audit` (`report.audit`,
  `ledger.audit_chain`). A strip across the top draws the whole chain: a tick per event, the
  traced record's events, failing events, and signed checkpoints. `1` traces a number: every
  PRISMA count opens to its records (`PrismaFlow.members`), and a record opens to a timeline of
  every ledger event that touched it, from the searches that found it (with the raw response) to
  its duplicates, each blind decision and when the AI's reading was shown after it, conflicts,
  adjudication, the full text (licence, PDF), and extracted values with their anchors; each event
  shows its seq, actor, time, and hashes, and links the artifacts it names. `2` checks the chain:
  `audit_chain` runs every check on every event (payload hash, event hash, the link to the event
  before, missing seqs) and every checkpoint (signature, head hash), and names each failing
  event and check with the stored and recomputed values; `c` signs a checkpoint at the head. `3`
  looks back: the PRISMA flows and the AI disclosure at any event, picked by seq, on the strip, or
  by name ("Title and abstract closed"), beside the head; `p` renders and keeps those reports at
  that event. `tests/app/test_audit.py` traces the Lesotho trial through every stage over HTTP and
  edits a decision as the database superuser, step by step up to a re-hashed tail and a deleted
  event, checking that each step is named and that a bundle sealed before still verifies while
  none can be sealed after. `tests/report/test_audit.py` checks the flow when title and abstract
  closed against the lead's status at that moment. `apps/web/e2e/audit.spec.ts` drives the
  screen, tampers through `e2e/tamper.ts`, and checks a screener is refused everywhere.
- Browser unit tests in Vitest browser mode on the mock, and screenshots in `apps/web/screenshots/`.

Left:

- The other report targets on the Reports screen, and the Risk of bias (WP-12 has no server
  implementation) and AI-exclusion audit screens; the full-text judge's per-criterion reading in
  adjudication; grounding exceptions.
- Adjudication shows a full-text column's evidence as counts, not the quotes on the page.
- Update searches: a second corpus ingest needs the corpus diff, so Search and Studies refuse it.
- Removing a person or a role, and renaming a review, are not built in the server either.
- A session break suggestion after a missed sentinel or a speed-up with an include-rate change.
- Third-party licence notices for the web bundle (`licenses:sync`).
- `scripts/screenshots.ts` still shoots the mock; point it at a seeded server.
- Audit: checkpoints are read from the database; a copy kept outside it (WORM storage) is not
  compared yet. The trace reads the whole chain per request, which is fine for one review's
  thousands of events but wants an index on `payload->>'item_ref'` at Embase scale.

## WP-17 Evaluation harness

Not a runtime module; lives in `apps/server/eval/`.

- Loaders for SYNERGY and the gold-data tiers G1 to G3 (DESIGN.md "Evaluation"), a runner that
  scores judges and extraction, and a report.
