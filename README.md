# DLQ Healing Agent

An autonomous **Kafka Dead Letter Queue self-healing and replay agent**. It listens to a DLQ, uses an LLM-driven *Remediation Planner* to decide what to do with each failed event, and then repairs and replays it, retries it later, or quarantines it for a human.

> **Status: Phases 1–6 of the build are complete.** Strategy A (schema repair) with the two-tier gate, the human approval API, the append-only audit log and distributed tracing, and Strategy C (quarantine), work end to end, and have been run for real (real OpenAI model, Kafka, Postgres and Jaeger; see [Real run findings](#real-run-findings)). Strategy B (transient retry) is not built yet, so **B events are still quarantined** (fail-closed) until Phase 7. Unit and integration tests use a *scripted* model. Sections marked 🚧 describe planned design. This README is updated at the end of every phase.

## Contents
- [AI-assisted development](#ai-assisted-development)
- [What it does](#what-it-does)
- [How it works](#how-it-works)
- [Architecture](#architecture)
- [Safety model](#safety-model)
- [Tech stack](#tech-stack)
- [Project layout](#project-layout)
- [Audit log and observability](#audit-log-and-observability)
- [Static analysis and hardening](#static-analysis-and-hardening)
- [Supply chain: SBOM and vulnerability scanning](#supply-chain-sbom-and-vulnerability-scanning)
- [Continuous integration](#continuous-integration)
- [Running it](#running-it)
- [Testing](#testing)
- [Configuration](#configuration)
- [Switching the LLM provider](#switching-the-llm-provider)
- [Database schema](#database-schema)
- [Roadmap](#roadmap)

## AI-assisted development
This project was built in AI-assisted sessions with Claude Code (Anthropic). The author set the goal, the architecture and the design decisions (for example the topic-adapter boundary, the hard-violation versus risk-flag split, the two-tier approval gate, and what to audit), and the AI wrote most of the code and tests. Because of that, the evidence offered for quality is what can be checked, not who typed it: the automated tests, the live runs against a real model described in [Live LLM evaluation](#live-llm-evaluation-opt-in) and [Real run findings](#real-run-findings), and the known limitations listed throughout this document.

## What it does

In an event-driven system, consumers fail on some events. Some failures are data problems (a field changed format), some are temporary (the database was briefly down), and some are junk that will never work. Normally all of them pile up in a DLQ until someone triages them by hand.

This service triages them automatically. For every event on `orders-dlq` it picks one of three strategies:

| Strategy | Trigger | What the agent does | Outcome |
|---|---|---|---|
| **A: Schema / data drift** | Deserialization or constraint errors, format mismatches | Inspects the topic contract with typed tools, proposes a typed patch, passes it through the Evaluator Gate | **Tier 1:** a mechanical, high-confidence fix is replayed to `orders-topic` with `x-repaired-by-agent: true`. **Tier 2:** anything resting on an assumption waits for a human (`PENDING_HUMAN_APPROVAL`) |
| **B: Transient infrastructure** | Connection resets, timeouts, downstream outage | Calls a health-check tool, schedules an exponential-backoff retry | Replays the unchanged event after the backoff |
| **C: Poison pill / unfixable** | Corrupt or malicious payload | Skips repair entirely | Writes to the PostgreSQL quarantine table for human audit |

## How it works

Implemented today: everything in the diagram except Strategy B (🚧 Phase 7). Every outcome, including each human decision, also writes a row to the [audit log](#audit-log-and-observability).

```
 orders-topic ──► main consumer ──(fails)──► orders-dlq
                                                │
                                      DLQ listener (this service)
                                                │  payload + headers + stack trace
                                                ▼
                                    Remediation Planner (Spring AI ChatClient)
                                                │  typed RemediationPlan
                         ┌──────────────────────┼───────────────────────┐
                         ▼                      ▼                       ▼
                 A: SchemaRepair         B: TransientRetry         C: Quarantine
              (schema tools → patch)    (health check → backoff)          │
                         │                      │                        │
             Evaluator Gate (hard / flags)  retry_schedule (Postgres)     │
              │            │         │              │ due                 │
        hard violation   risk flag   clean          ▼                     ▼
              │         or low conf.    │        replay            quarantine_event
              ▼            ▼            ▼                          (Postgres)
         quarantine   Tier 2: human  Tier 1: replay to orders-topic
                      approves via   (x-repaired-by-agent: true)
                      REST API ──► replay (+ x-approved-by)
```

Fallbacks, all of which end in quarantine:
- The planner throws or returns no plan (**implemented**).
- The planner's output is unparseable, has an unknown strategy or an out-of-range confidence, or confidence is below `dlqagent.planner.min-confidence` (**implemented**).
- Strategy A: the repair agent declines, errors, returns garbage, proposes a patch that cannot be applied, or proposes a patch with a hard violation (**implemented**). Patches with only risk flags are not quarantined; they go to Tier 2.
- The event is not a JSON object, so it is never sent to the repair model (**implemented**).
- The `original-topic` header names a topic other than `orders-topic` (**implemented**).
- The event already carries `x-repaired-by-agent` and failed again (**implemented**, loop guard).
- A plan names Strategy B, which is not built yet (**implemented**, temporary).
- 🚧 Strategy B exceeds its maximum retries.

### Event flow in detail (implemented part)
1. `DlqListener` consumes `orders-dlq` with manual acknowledgement.
2. `FailureContextFactory` builds a `FailureContext`: payload, key, headers, stack trace, partition, offset. It reads the standard Spring dead-letter headers `kafka_dlt-original-topic` and `kafka_dlt-exception-stacktrace`.
3. The correlation ID comes from the `x-correlation-id` header, or a UUID is generated if absent. It is placed in the logging MDC (`[cid=...]` in every log line) and stored with the quarantine row.
4. `DlqTriageService` asks the `RemediationPlanner` port for a `RemediationPlan` (see [The Remediation Planner](#the-remediation-planner)) and does an exhaustive `switch` on it.
5. The outcome is persisted (a replay, a Tier 2 hold, or a quarantine), an audit row is written, and only then is the Kafka offset committed.

### How the model learns about the failure
The model is *given* the failure details; it does not look them up. The main service's dead-letter publisher (Spring Kafka's `DeadLetterPublishingRecoverer`) puts the Java stack trace in the `kafka_dlt-exception-stacktrace` header. `FailureContextFactory` reads it into `FailureContext.stackTrace`, and `FailurePrompt` places the payload, headers and stack trace in the prompt between random delimiter lines (the model is told they are untrusted data). The stack trace is cut at `planner.max-stack-trace-chars` (2000).

Limits: if the producer does not set that header the model sees `(none)`; a long trace loses its tail (where the "Caused by" chain often is); and the other `kafka_dlt-*` headers, including the exception message header, are not passed on (the stack trace normally starts with the exception class and message). The planner has no tools, and the repair agent's two tools (`describeTable`, `validateOrderPayload`) concern the data contract, not the failure. An observability tool is a roadmap idea (row 9).

### The Remediation Planner
`SpringAiRemediationPlanner` implements the `RemediationPlanner` port with Spring AI's `ChatClient`. In this phase it only **classifies**; it has no tools and cannot act on anything.

1. It sends the model a system prompt (`src/main/resources/prompts/planner-system.txt`) defining the three strategies, and a user message containing the original topic, payload, headers and stack trace.
2. It asks for structured output (`.entity(PlannerDecision.class)`): `strategy`, `confidence` (0.0–1.0) and `rationale`.
3. It validates the decision and maps it to a `RemediationPlan`. Anything doubtful becomes `Quarantine`.

| Model output | Result |
|---|---|
| `SCHEMA_DRIFT`, confidence ≥ threshold | `SchemaRepair` |
| `TRANSIENT`, confidence ≥ threshold | `TransientRetry` |
| `POISON_PILL` | `Quarantine` (rationale kept as the reason) |
| Confidence below `planner.min-confidence` | `Quarantine`, even for A or B |
| Missing or unknown strategy, confidence outside 0–1 or NaN, non-JSON, model or network error | `Quarantine` |

The `plan` method never throws; the triage service guards it as well.

**Treating the payload as hostile.** The failed event is attacker-controlled text, so:
- It is passed as a template *parameter*, never as template text. Spring AI parses `{...}` in prompt text as variables, and JSON payloads are full of braces.
- It sits between delimiter lines containing a random per-request token, so the payload cannot fake the boundary.
- The system prompt tells the model to treat it as data and to count instructions found in it as evidence for `POISON_PILL`.
- Payload and stack-trace lengths are capped (`planner.max-payload-chars`, `planner.max-stack-trace-chars`), and the stored rationale is capped at 500 characters.
- The model's output is only a classification. Even a successfully hijacked answer can at worst pick a different strategy, and strategy A is still gated by the deterministic Evaluator (🚧) before anything is replayed.

### Strategy A: schema repair and replay
`SchemaRepairAgent` (the `PayloadRepairer` port) handles `SchemaRepair` plans. It is **generic**: everything topic-specific comes from a `TopicAdapter`.

**Topic adapters.** A `TopicAdapter` is the boundary between the generic engine and one topic's domain. It supplies:
- `contract()`: the prompt text describing the target contract and its tools;
- `tools()`: the read-only `@Tool` objects the model may call for this topic;
- `editableFields()` and `immutableFields()`: the only fields a patch may touch, and the identity fields it never may;
- `validate(json)`: a standalone contract check, which is also what the model's validation tool runs;
- `evaluate(original, candidate, key)`: the authoritative deterministic check a repair must pass before replay.

`TopicAdapterRegistry` resolves the adapter from the event's `original-topic`. A topic with no adapter cannot be repaired and the model is never called. Two adapters for one topic fail startup. The reference implementation is `OrderTopicAdapter` (package `orders`): an `OrderEvent` (`orderId`, `customerId`, `amount`, `currency`, `status`, `createdAt`) backed by the `orders` table. The model's own use of a validation tool is advisory; the engine always re-runs `adapter.evaluate` itself, so a model that skips or misreads the tool cannot bypass it.

1. **Parse.** The payload must be a JSON object (strict: no duplicate keys, no trailing text). Otherwise the model is never called.
2. **Ask the repair model**, with the adapter's contract in the system prompt and the adapter's tools attached. For orders these are two read-only tools (`OrderSchemaTools`):
   - `describeTable(tableName)`: column types, lengths and constraints from the Postgres catalog. Only the allow-listed table `orders` is accepted.
   - `validateOrderPayload(json)`: runs the gate's schema and business rules and returns the violations, so the model can test its own idea.
3. **The model returns a `PatchProposal`, not a payload**: `repairable`, `confidence`, `summary`, `edits` (field plus new value) and `dropFields`. `field` is a JSON field name and must be on the adapter's editable list (for orders: `amount`, `currency`, `status`, `createdAt`). `PatchApplier` rejects an edit to anything else, including the identity fields `orderId` and `customerId`, and refuses to drop identity fields. (Before Phase 4b this was an enum, which also constrained the model's output schema. It is now a string validated in Java, which keeps the engine topic-agnostic. The editable list is generated into the prompt from the same set the applier enforces.)
4. **Java applies the patch** (`PatchApplier`) to a copy: at most 8 edits, no duplicate edits, values capped at 64 characters, and values converted by the adapter's declared field type (a `DECIMAL` field must parse as a plain decimal).
5. **The Evaluator Gate decides** (`adapter.evaluate`; for orders, `OrderContractValidator`, plain Java, no model). It returns two separate lists: **hard violations** (the patch must never be replayed, even by a human) and **risk flags** (valid, but resting on an assumption a human should confirm):

   | Rule | Checks | Result |
   |---|---|---|
   | Schema | Required fields and types; id format; `amount` is a JSON number, > 0, ≤ 2 decimals, < 10¹⁰; `currency` is a real ISO-4217 code; `status` is a known value; `createdAt` is ISO-8601 with offset and not in the future; no unknown fields | hard |
   | Identity | `orderId` and `customerId` must exist in the original and be unchanged; `orderId` must equal the Kafka key | hard |
   | Amount value | The numeric value of `amount` must not change (`"19.99 USD"` → `19.99` is fine, `29.99` is rejected) | hard |
   | Unverifiable amount | Original amount text with no number, two numbers, or thousands separators, so the value cannot be compared | flag |
   | Filled-in field | A field absent in the original was populated by the agent (for example a missing `currency`) | flag |
   | Rename | Old field dropped, new field set; the mapping cannot be verified | flag |
   | Mapping | `status` or `currency` changed to something other than a case or whitespace variant (`dispatched` → `SHIPPED`, `$` → `USD`) | flag |
   | Ambiguous date | `03/04/2025` style dates where day and month could be swapped | flag |
   | Reformatted date | A non-ISO `createdAt` was converted, which assumes a time of day and zone | flag |

   Not flagged (mechanical, so eligible for Tier 1): text amount to the same number, case normalization of `status` and `currency`, and dropping an unknown extra field.
6. **Route by tier** (`DlqTriageService`, deterministic, see [The two-tier gate](#the-two-tier-gate)).
7. **Replay.** A Tier 1 candidate (or an approved Tier 2 proposal) is published to the original topic by `KafkaEventReplayer` with the original key and headers plus `x-repaired-by-agent: true`, `x-correlation-id` and `x-repair-summary`, and `x-approved-by` when a human approved it. The send must be acknowledged by the broker. Any copy of these audit headers on the failed event is stripped first, so they cannot be spoofed forward.

Edge cases deliberately handled:
- **Hard violation:** quarantined as `SCHEMA_DRIFT` (`PENDING_REVIEW`) with the violations as the reason, and nothing is replayed.
- **Repair loop:** an agent-repaired event that fails again goes straight to quarantine, without planning or repair.
- **Spoofed topic:** replay only targets `orders-topic`; any other `original-topic` header value is quarantined.
- **No-op patch:** if the proposal changes nothing, the failure is not a payload problem and it is quarantined.
- **Broker down during replay:** the error propagates, the offset is not committed and the listener retries.

Known limitations:
- Replay is **at-least-once**. If the process dies after the broker acknowledges the replay but before the DLQ offset is committed, the event is replayed twice. The downstream consumer must be idempotent (`orders.order_id` is the primary key).
- A rename repair (old field dropped, new field set) is checked for structure, but the gate cannot prove the renamed field's *value* is the right one. That is why renames are risk-flagged and need a human.
- `JdbcSchemaInspector` (infrastructure) still hard-codes the `orders` allow-list. It is deliberate defense in depth, but it means a second topic that needs schema inspection requires a change there. Decoupling it would either weaken that check or introduce circular wiring.
- Only the adapter's editable fields can be repaired (for orders, four). A wrong `customerId` is always a human decision.
- The `orders` table exists as the schema reference for the tools; this service does not write to it. Applying events is the job of the main consumer.

### The two-tier gate
After the hard checks pass, the triage service routes the repair with a deterministic policy (no model involved):

| | Condition | Outcome |
|---|---|---|
| **Tier 1** | no risk flags **and** the repair's confidence ≥ `gate.auto-replay-min-confidence` (0.9) | replayed automatically |
| **Tier 2** | any risk flag, **or** confidence below the threshold | a `quarantine_event` row with status `PENDING_HUMAN_APPROVAL`, holding the original payload, the proposed payload, the risk flags and the repair summary. Nothing is published |
| **Rejected** | any hard violation | quarantined (`PENDING_REVIEW`) |

The planner's confidence is not part of this decision: it only has to clear `planner.min-confidence` (0.7) for the event to be routed to repair at all. It measures "is this schema drift", not "is this patch safe", and the real planner's value for the same kind of event varies (0.80 to 0.90 in real runs). Confidence can only *demote* a repair to Tier 2; it can never override a risk flag. A model that says "1.0" about a guessed date still goes to a human. Tier 1 is deliberately narrow (see the "not flagged" list above), so most real drift will land in Tier 2 until you widen what counts as mechanical.

### Human approval API
Tier 2 rows are reviewed through a small REST API, protected by HTTP Basic auth (Spring Security). The reviewer's name is recorded on every decision and, for approvals, on the replayed record as `x-approved-by`.

| Endpoint | Purpose |
|---|---|
| `GET /api/approvals?status=PENDING_HUMAN_APPROVAL&limit=50` | The review queue (default status shown; any status can be listed; limit capped at 100) |
| `GET /api/approvals/{id}` | One item: original and proposed payload, **field-level diff**, risk flags, repair summary, reason |
| `POST /api/approvals/{id}/approve` JSON body, `{}` or `{"notes": "..."}` (a body is required, notes are optional) | Re-runs the hard checks on the stored proposal, replays it, marks it `APPROVED_REPLAYED` |
| `POST /api/approvals/{id}/reject` body `{"notes": "..."}` (notes **required**) | Marks it `REJECTED`; nothing is replayed |

```bash
export APPROVAL_PASSWORD=choose-something
curl -u reviewer:$APPROVAL_PASSWORD localhost:8080/api/approvals
curl -u reviewer:$APPROVAL_PASSWORD -X POST localhost:8080/api/approvals/<id>/approve \
     -H 'Content-Type: application/json' -d '{"notes":"confirmed day-first with the customer"}'   # a JSON body is mandatory; '{}' is enough
```

Behaviour worth knowing:
- **Re-validation on approve:** the hard rules run again on the stored proposal. If it no longer passes (rules changed, or the row was edited), the API returns `409` and nothing is replayed. Risk flags are not re-checked, because accepting them is what the reviewer is doing.
- **No double replay:** each decision runs in one transaction holding a row lock (`SELECT ... FOR UPDATE`). Two simultaneous approvals produce one `200` and one `409`, and exactly one replayed record (tested).
- **Broker failure:** if Kafka does not acknowledge the replay, the API returns `502` and the item stays pending.
- **Status codes:** `401` no or wrong credentials, `404` unknown id, `409` wrong state or failed re-validation, `400` malformed input.
- **Security:** everything except `/actuator/health`, `/api/approvals/**` and `/api/audit/**` is denied. CSRF tokens are off because this is a stateless Basic-auth API for scripts, but note that a browser can cache Basic credentials and resend them cross-site, so "no cookies" is *not* a defence by itself. The defence is that every state-changing endpoint accepts only `application/json` **with a body**: a cross-site HTML form cannot send that content type, and a cross-site `fetch` with it needs a CORS preflight, which this service never approves. (An earlier version let approve take an optional body, so a body-less cross-site POST would have been accepted; the SAST work found and closed that, see below.) There is a single reviewer account whose password has no default (`APPROVAL_PASSWORD`); the app refuses to start without it.

Security limitations to be aware of: HTTP Basic sends credentials with every request, so it must run behind TLS in any real deployment; there is no rate limiting or lockout; and one shared account means "who approved" is only as granular as the account. A production system would use OIDC with individual identities.

### Reliability behaviour
- **No event loss:** the offset is committed only after triage succeeds. If Postgres is down, the listener retries with exponential backoff (1s doubling to 30s, unbounded) and does not skip the record.
- **Idempotent:** the quarantine row ID is derived from `dlqTopic + partition + offset`, and the insert uses `ON CONFLICT DO NOTHING`, so redelivery never creates duplicates.
- **Hostile payloads:** Postgres cannot store NUL (`U+0000`) in `text` or `jsonb`. Such a payload is quarantined with the NUL replaced by `U+FFFD`, rather than failing the insert and blocking the partition. A null (tombstone) value is stored as an empty payload.
- **Hostile headers and keys:** Kafka keys and the original-topic header are producer-controlled and unbounded, and the listener retries a failing insert forever, so a value the database rejects would block its partition permanently (reproduced in a test before the fix, where it also stalled an unrelated event behind it on the same partition). Those columns are therefore `TEXT` (V6), and the key is stored whole because replay needs the exact key. The correlation id, the one value that lives in a bounded column, goes through a boundary check: only `[A-Za-z0-9._:-]{1,64}` is trusted, anything else (too long, spaces, line breaks, control characters) is replaced by a generated UUID.
- **Log injection:** untrusted text (payload-derived reasons, exception messages, model output, header values) is passed through `LogSafe.clean` before it reaches a log line: control characters, including CR, LF and the Unicode line separators, become `_`, and the length is capped at 500.

## Audit log and observability

### The audit log (`dlq_audit_log`)
One row per execution: every triage outcome (replayed, held for approval, quarantined) and every human decision (approved, rejected). It records *what was decided and why*:

| Column | Content |
|---|---|
| `event_id`, `correlation_id`, `trace_id` | The DLQ record's identity, the correlation ID, and the distributed trace id that joins this row to the trace |
| `stage`, `actor` | `TRIAGE` by `agent`, or `APPROVAL` by the reviewer's username |
| `strategy`, `planner_confidence`, `planner_rationale` | The planner's classification and its stated reason |
| `repair_summary`, `repair_confidence` | The repair agent's explanation and confidence (also kept when the repair was rejected, to debug prompts) |
| `tool_calls` | Which tools the model called: name, success, duration. **Never arguments or results** |
| `changes` | Field-level diff, original to proposed (what was attempted, even if rejected) |
| `risk_flags`, `tier`, `outcome`, `outcome_reason`, `duration_ms` | The policy decision and its result |

Design points:
- **Append-only, enforced by the database.** A trigger rejects `UPDATE` and `DELETE` (tested). A superuser can still drop the trigger, so this is tamper-resistance against application bugs, not against a hostile DBA.
- **No raw prompts or tool arguments.** Payloads can contain personal data, so the audit stores reasoning summaries, tool-call summaries and diffs only. Note that a diff does contain the before and after values of the fields that changed, so apply the same retention rules to this table as to the data it describes.
- **Atomic with the hold.** A quarantine or Tier 2 row and its audit row are written in one transaction, so a held event can never lack an audit record. A Tier 1 replay is audited right after the publish; a Tier 2 decision's audit row commits in the same transaction as the decision.
- **Idempotent.** The row id is derived from the DLQ record and stage, so reprocessing after a crash cannot write a second row.
- **A decision that did not happen is not audited as one.** A rejected approval attempt (`409`) or a failed replay (`502`) writes nothing; they are visible in logs and traces.
- **Queryable:** `GET /api/audit?correlationId=...&limit=50` (same Basic auth) returns an event's full trail, oldest first. For example, a held event followed by its approval shows two rows with the same `event_id`.

### Tracing
Every run is traced with Micrometer Tracing bridged to OpenTelemetry. Spans are always created and linked to audit rows; exporting them is opt-in.

A real triage trace (observed in Jaeger) contains, nested:

`orders-dlq receive` (Spring Kafka, continues the producer's trace) → `dlq.triage` (outcome and tier as tags) → `dlq.plan` → `spring_ai chat_client` → `chat gpt-4o-mini` → `http post`; then `dlq.repair` → `spring_ai chat_client` → the model calls plus one `tool_call describe-table` / `tool_call validate-order-payload` span per tool call; then `dlq.replay` → `orders-topic send`.

An approval is its own trace (`http post /api/approvals/{id}/approve` → security spans → `dlq.approval` → `dlq.replay` → `orders-topic send`). The replayed Kafka record carries a `traceparent` header, so a downstream consumer's trace continues from the approval. The `trace_id` stored in each audit row is the same id Jaeger shows.

To view traces:
```bash
docker compose --profile tracing up -d       # adds Jaeger (UI http://localhost:16686, OTLP on :4318)
TRACING_EXPORT=true mvn spring-boot:run      # OTLP_TRACING_ENDPOINT overrides the collector URL
```
Log lines also carry `[trace=...]` next to `[cid=...]`. Sampling is 100% because this is a low-volume control plane. Baggage propagation is switched off (`management.tracing.baggage.enabled=false`): it is unused, and the baggage header is producer-controlled input (see the OpenTelemetry advisory above). Every `Observation` also feeds Micrometer timers (for example `dlq.triage` tagged by outcome and tier), but no metrics endpoint is exposed yet.

## Static analysis and hardening
`mvn verify` runs **SpotBugs with the FindSecBugs security rules** (effort Max, threshold Low) after the tests and **fails the build on any finding**. The only things allowed through are the entries in `spotbugs-exclude.xml`, each with its reason written next to it: the deliberate CSRF decision, informational endpoint listings, a constructor that stores injected collaborators, an intentional text-block newline, and a rethrow that must not be swallowed.

The first scan found 31 findings. Triage, honestly:

| Finding | Verdict | What was done |
|---|---|---|
| `SPRING_CSRF_PROTECTION_DISABLED` | **Real, and my reasoning was wrong.** I had justified it with "no cookies, so no ambient credential", which is false for browsers and Basic auth. A new test then showed a body-less cross-site POST to `approve` was accepted | State-changing endpoints require a JSON body and content type; justification corrected; covered by `ApprovalFlowIT` |
| `CRLF_INJECTION_LOGS` (8) | **Real**: payload-, model- and header-derived text was logged raw | `LogSafe.clean` at every site; correlation ids validated at the boundary |
| Oversized key or header values (found while triaging the logging findings, not reported by SpotBugs) | **Real, severe**: a value over a column limit would fail the insert forever and block the partition | Columns widened to `TEXT`, correlation id validated, test added that fails without the fix |
| `IMPROPER_UNICODE` | **Real**: `equalsIgnoreCase` treats the long s as an `s`, so a status of `ſhipped` counted as a harmless case change and could reach Tier 1 | ASCII-only case comparison; test added |
| `DCN_NULLPOINTER_EXCEPTION` | Fair: code caught `NullPointerException` instead of checking for null | Explicit null check |
| `EI_EXPOSE_REP` and `REP2` (10) | Mixed: several records stored mutable collections or maps without copying; the rest were injected beans | Defensive copies in three records; injected-bean case excluded with a reason |
| `MS_EXPOSE_REP` | Real but minor: a shared, mutable `ObjectMapper` was exposed | Mapper hidden behind `StrictJson.write` |
| `CT_CONSTRUCTOR_THROW` (2) | Theoretical (finalizer attacks) | Classes made `final` |
| `SPRING_ENDPOINT`, `THROWS_METHOD_THROWS_RUNTIMEEXCEPTION`, `VA_FORMAT_STRING_USES_NEWLINE` | Informational or intentional | Excluded, with reasons |

What this does **not** cover: dependency vulnerabilities (a software-composition scan such as OWASP Dependency-Check or Dependabot is on the roadmap), runtime or dynamic testing, secrets scanning, or the LLM-specific risks, which are handled by design (typed tools, the deterministic gate, the human tier) and by the prompt-injection tests rather than by a scanner.

## Supply chain: SBOM and vulnerability scanning
The app ships **120+ third-party libraries** (about 20 declared in the `pom.xml`, the rest transitive), and SpotBugs only analyzes our own code, so a separate scan covers the dependencies.

**SBOM.** Every build writes a CycloneDX software bill of materials to `target/classes/META-INF/sbom/application.cdx.json`, which is packaged inside the jar. It comes from the `cyclonedx-maven-plugin` declared in the `pom.xml`, which inherits Spring Boot's managed configuration. (Boot's parent POM only *manages* this plugin; it does not run unless the project declares it.)

**Scan.** `scripts/sca.sh` rebuilds from clean, then:
1. **Syft** reads the built jar and writes a second CycloneDX SBOM (what is actually shipped);
2. **Grype** scans both SBOMs against its vulnerability database and fails on **High or above**, minus the reviewed entries in `.grype.yaml`.

Both tools run as pinned Docker images (Syft 1.55.0, Grype 0.121.0), so nothing is installed on the host. Requires Docker, Java 21 and Maven. The vulnerability database changes daily, so the result can change without any code change; that is why this is a separate gate (scheduled daily in CI, see [Continuous integration](#continuous-integration)) and **not** part of `mvn verify`, which stays deterministic.

**Baseline and what was done.** The first scan found **27 known vulnerabilities (7 Critical, 8 High, 11 Medium, 1 Low)**. Triage, by what matters for this app:

| Library | Advisory type | Relevance here | Action |
|---|---|---|---|
| Jackson 2.21.4 | parser denial of service (several High) | **Reachable**: this service parses untrusted DLQ payloads | Upgraded to 2.21.7 |
| OpenTelemetry 1.49.0 | unbounded allocation parsing W3C baggage headers | **Reachable**: Kafka trace headers come from producers | Upgraded to 1.62.0 **and** baggage propagation turned off (unused) |
| Tomcat 10.1.55 | DIGEST and FORM authenticator bypass (Critical) | Not reachable (Spring Security does the authentication), but a fix exists | Upgraded to 10.1.61 |
| PostgreSQL JDBC 42.7.11, lz4-java 1.10.1, log4j-api 2.24.3 | channel-binding downgrade, temp-file race, minor | Low exposure, fixes exist | Upgraded to 42.7.12, 1.11.4, 2.25.5 |
| Spring Framework 6.2.19 (webmvc, webflux) | 4 Critical: XsltView, SSE rendering, WebFlux functional endpoints | None of those features are used (checked by search) | **No fixed release exists** (6.2.19 is the newest 6.2.x). Ignored with a written reason |
| MCP Java SDK 0.18.3 | DNS rebinding in the MCP server transport | No MCP server or client is used; arrives via `spring-ai-client-chat` | Fix is a major jump that Spring AI 1.1.8 was not built against. Ignored with a written reason |

Result: **27 findings down to 5, all unfixable today and each documented in `.grype.yaml`** (revisit them on every Spring Boot or Spring AI upgrade). The upgrades are plain overrides in the `pom.xml` properties (`tomcat.version`, `jackson-bom.version`, `postgresql.version`, `log4j2.version`, `opentelemetry.version`, plus a managed `lz4-java`), each with a comment; remove one when Boot's own managed version catches up. The gate was checked in both directions: without the ignore list it exits 2, with it 0. The full test suite and SpotBugs pass on the upgraded versions.

**Lessons from doing it:**
- **Two SBOMs, because they disagree.** An earlier run showed a finding in one SBOM and not the other. The cause was a stale SBOM file left in `target/` by a previous build, which reported an old OpenTelemetry version even though the jar already contained the new one. The script therefore always rebuilds from clean. Scanning the built jar (Syft) is the ground truth; the Maven-generated SBOM is a cross-check.
- **A "fixed in 10.1.58" advisory does not mean 10.1.58 exists.** That Tomcat version was never published to Maven Central, but 10.1.59 to 10.1.61 were, and they include the fix. Check the repository, not just the advisory.

**Also in the repo:** `.github/dependabot.yml` (weekly Maven and Docker update pull requests; major Spring Boot and Spring AI upgrades are deliberately excluded). Dependabot *security alerts* must also be switched on in the GitHub repository settings (Settings, Code security).

**Not covered:** license compliance, reachability analysis beyond the manual check above, signing or provenance of the build (for example SLSA attestations), publishing the SBOM as a release artifact, and scanning of a container image (there is none yet; the app runs as a plain jar).

## Continuous integration
Two GitHub Actions workflows in `.github/workflows/`:

| Workflow | Runs on | What it does |
|---|---|---|
| `ci.yml` | every push to `main`, every pull request | `mvn -B verify` on Java 21: unit tests, Testcontainers integration tests (Docker is available on the runner), and the SpotBugs + FindSecBugs gate. Uploads the test reports if the build fails. The opt-in live-LLM evaluation is skipped because no API key is configured |
| `sca.yml` | every push to `main`, every pull request, **daily at 05:23 UTC**, and on demand from the Actions tab | `scripts/sca.sh`: clean build, Syft SBOM of the jar, Grype scan of both SBOMs, fails on High or above. Uploads the SBOMs as a build artifact (30 days). The Grype database is cached between runs |

Why a daily schedule: the vulnerability database changes every day, so a dependency that was clean yesterday can be flagged today with no code change. A failed scheduled run is how that surfaces; GitHub emails the person who last edited the cron line, so keep Actions notifications on (Settings, Notifications).

Choices:
- **Actions are pinned to exact commit SHAs** (with the version in a comment), not to moving tags, because the workflows run third-party code. Dependabot's `github-actions` ecosystem is not enabled yet, so bumping them is manual for now.
- **Least privilege:** both workflows request only `contents: read`.
- **Concurrency:** a newer run on the same ref cancels the older one.
- **No secrets are used**, and the live-LLM evaluation deliberately never runs in CI (it costs money and depends on the model).

Status and limits, stated plainly:
- **The workflows have been linted with `actionlint` (clean; a deliberately broken control file was caught) and the script they call was run locally, but they have not yet run on GitHub.** The first push will be their real test, and Testcontainers-in-Actions may need a tweak.
- A private repository on the free GitHub plan has a monthly Actions-minutes quota, and branch protection (making these checks mandatory before merging) is not available for private repositories on the free plan. The checks run and report, but nothing blocks a merge.
- GitHub can switch off scheduled workflows after a long period without repository activity; if the daily scan goes quiet, check the Actions tab.

## Architecture

Four layers, with dependencies pointing inward:

| Layer | Package | Responsibility |
|---|---|---|
| Domain | `domain` | Records and ports (`RemediationPlanner`, `PayloadRepairer`, `EventReplayer`, `TopicAdapter`, `SchemaInspector`, `QuarantineRepository`). No Spring, Kafka or LLM dependencies. |
| Agent | `agent` | Generic engine: triage orchestration, planner, repair agent, adapter registry, patch applier. Knows nothing about any specific topic. |
| Topic adapters | `orders` | `OrderTopicAdapter`, its `@Tool` class, the Evaluator Gate rules and the `OrderEvent` contract. A new topic is a new adapter package. |
| JSON | `json` | `StrictJson`: the one strict parsing dialect for untrusted payloads. |
| Messaging | `messaging` | DLQ listener, header and correlation-ID handling, replay publisher. |
| Infrastructure | `infrastructure` | Kafka and Postgres config, typed properties, JDBC adapters (quarantine, schema inspector). |

The planner's output is a sealed `RemediationPlan` (`SchemaRepair | TransientRetry | Quarantine`), so a Java 21 `switch` over it is exhaustive and the compiler flags any handler missing a new strategy. Listeners run on virtual threads (`spring.threads.virtual.enabled=true`).

## Safety model

The LLM only proposes. It never executes.

1. **Typed tools only.** The model can call only whitelisted, parameterized `@Tool` methods. Each strategy gets its own tool set, so it cannot reach tools outside its path. There is no raw SQL and no raw Kafka access.
2. **Typed output.** The plan must parse into a `RemediationPlan` record. Anything else is quarantined.
3. **Deterministic gate.** The Evaluator Gate (for orders: schema, business rules, identity fields, amount preservation, plus risk flags for assumptions) is plain Java outside the LLM, supplied by the topic adapter. Nothing is replayed without passing its hard checks.
4. **Structurally limited edits.** The model returns edits to four whitelisted fields, never a payload. Identity fields are not representable in an edit.
5. **Closed loops and targets.** Replays go only to the configured main topic, and an agent-repaired event that fails again is quarantined, never re-repaired.
6. **Human in the loop for assumptions.** A repair with any risk flag, or with confidence below the threshold, is never auto-replayed. Confidence can only demote, never promote.
7. **Authenticated approvals.** Approving publishes to a production topic, so the approval API requires credentials, re-validates the proposal, locks the row, and records the reviewer.
8. **Bounded.** Edit-count, value-length and confidence limits, plus fail-closed handling everywhere, route doubtful cases to quarantine.

## Tech stack

- Java 21 (records, sealed types, virtual threads)
- Spring Boot 3.5.16
- Spring AI 1.1.8 (`ChatClient`, `@Tool`), OpenAI by default
- Apache Kafka (via Spring Kafka), PostgreSQL 16, Flyway
- JUnit 5, Testcontainers (Kafka and PostgreSQL)
- Maven

## Project layout

```
dlq-healing-agent/
├── pom.xml                         Maven build; LLM provider profiles; Failsafe for *IT tests
├── docker-compose.yml              Local Kafka (KRaft) and PostgreSQL (+ optional Jaeger)
├── spotbugs-exclude.xml            reviewed SAST exclusions, each with its reason
├── .grype.yaml                     reviewed SCA ignores (unfixable advisories), each with its reason
├── scripts/sca.sh                  SBOM (Syft) + vulnerability scan (Grype) via pinned Docker images
├── .github/dependabot.yml          weekly dependency update pull requests
├── .github/workflows/ci.yml        build, tests and SAST on push and pull request
├── .github/workflows/sca.yml       SBOM and vulnerability scan on push, pull request and daily
├── src/main/java/com/example/dlqagent/
│   ├── DlqAgentApplication.java
│   ├── domain/
│   │   ├── FailureContext            failed event + metadata; stable eventId()
│   │   ├── RemediationPlan           sealed: SchemaRepair | TransientRetry | Quarantine
│   │   ├── RemediationStrategy       SCHEMA_DRIFT | TRANSIENT | POISON_PILL
│   │   ├── QuarantineRecord / QuarantineStatus   a quarantine entry or a Tier 2 approval request
│   │   ├── Evaluation                hard violations + risk flags returned by an adapter
│   │   ├── AuditEntry / AuditLog     one audit row and its port; ToolCallRecord, FieldChange
│   │   ├── ReplayRequest             everything needed to publish a repaired event
│   │   ├── TopicAdapter              boundary to topic-specific contract, tools, rules
│   │   ├── RepairResult              sealed: Repaired | Unrepairable
│   │   └── ports: RemediationPlanner, PayloadRepairer, EventReplayer,
│   │             SchemaInspector, QuarantineRepository (save, find, lock, review)
│   ├── agent/
│   │   ├── DlqTriageService          plan → exhaustive switch → Tier 1 replay / Tier 2 hold / quarantine; loop guard
│   │   ├── SpringAiRemediationPlanner  ChatClient classifier → RemediationPlan; fails closed
│   │   ├── PlannerDecision           raw, untrusted structured LLM output
│   │   ├── SchemaRepairAgent         Strategy A, generic: adapter contract + tools → PatchProposal → apply → adapter.evaluate
│   │   ├── TopicAdapterRegistry      topic → adapter; no adapter means no repair
│   │   ├── PatchProposal / FieldEdit typed patch language (fields validated against the adapter)
│   │   ├── PatchApplier              deterministic, bounded patch application
│   │   ├── RecordingToolCallback     wraps each tool to record name, success and duration (no arguments)
│   │   └── FailurePrompt             fenced, truncated prompt rendering
│   ├── orders/                       the reference topic adapter
│   │   ├── OrderTopicAdapter         implements TopicAdapter for orders-topic
│   │   ├── OrderContractValidator    the order Evaluator Gate rules; EvaluationResult
│   │   ├── OrderSchemaTools          the two read-only @Tool methods
│   │   └── OrderEvent / OrderStatus  the order contract
│   ├── audit/AuditController         GET /api/audit?correlationId=
│   ├── observability/                TraceContext (trace id for audit rows), LogSafe (log-injection guard)
│   ├── approval/                     human review of Tier 2 proposals
│   │   ├── ApprovalController        REST: list, get, approve, reject
│   │   ├── ApprovalService           transactional, row-locked decisions; re-validates on approve
│   │   └── ApprovalView, exception handler, small exceptions
│   ├── json/                         StrictJson (strict parsing), JsonDiff / FieldChange (field-level diff)
│   ├── messaging/
│   │   ├── DlqListener               @KafkaListener on the DLQ, MDC, manual ack
│   │   ├── FailureContextFactory     ConsumerRecord → FailureContext
│   │   ├── KafkaEventReplayer        replay to orders-topic with audit headers
│   │   └── DlqHeaders                header name constants
│   └── infrastructure/
│       ├── DlqAgentProperties        typed, validated config
│       ├── KafkaTopicConfig          declares orders-topic and orders-dlq
│       ├── KafkaConsumerConfig       unbounded backoff error handler
│       ├── SecurityConfig            Basic auth, deny-by-default, one reviewer account
│       ├── JdbcQuarantineRepository  idempotent insert, review queries, row locking
│       ├── JdbcAuditLog              idempotent append, query by correlation id
│       ├── PgText                    NUL sanitizing shared by the JDBC adapters
│       └── JdbcSchemaInspector       read-only catalog lookups, table allow-list
├── src/main/resources/
│   ├── application.yml
│   ├── prompts/planner-system.txt  planner system prompt
│   ├── prompts/repair-system.txt   generic repair agent instructions
│   ├── prompts/orders-contract.txt order contract, tools and rules shown to the repair model
│   └── db/migration/               V1 quarantine_event, V2 retry_schedule, V3 orders, V4 tier-2 columns, V5 dlq_audit_log, V6 unbounded text columns
└── src/test/java/com/example/dlqagent/
    ├── AbstractIntegrationTest.java  shared Testcontainers base class
    ├── InfrastructureSmokeIT.java    tables, topics, config binding
    ├── AuditFlowIT.java              audit rows per outcome, append-only, NUL safety, atomic rollback, real spans
    ├── ApprovalFlowIT.java           Tier 2 end to end: held, reviewed via the secured API, replayed or rejected
    ├── QuarantineFlowIT.java         DLQ → listener → planner (stub) → triage → Postgres, idempotency, NUL payload
    ├── support/ScriptedChatModel.java  deterministic fake LLM that records prompts
    ├── support/TestProps.java, MainTopicReader.java, TracingTestConfig.java   test helpers (in-memory span exporter)
    ├── support/StubLlmConfig.java      swaps in the fake ChatModel (not under profile live-llm)
    ├── agent/SpringAiRemediationPlannerTest.java
    ├── agent/LivePlannerEvalIT.java    opt-in, real model, skipped by default
    ├── agent/DlqTriageServiceTest.java
    └── messaging/FailureContextFactoryTest.java
```

## Running it

### Prerequisites
- JDK 21 (the build targets 21 even if a newer JDK is installed; point `JAVA_HOME` at 21)
- Maven 3.9+
- Docker (for local infra and for tests)
- An OpenAI API key (needed to run the app from Phase 3 onward; not needed for tests)

### Start local infrastructure
```bash
docker compose up -d        # Kafka on :9092, PostgreSQL on :5432
```
Troubleshooting:
- **Port 5432 already in use** (another Postgres): change the host port mapping in `docker-compose.yml` and set `DB_URL=jdbc:postgresql://localhost:<port>/dlqagent`.
- **`image ... does not provide the specified platform (linux/amd64)`**: your shell has `DOCKER_DEFAULT_PLATFORM=linux/amd64` set while the image on disk is arm64 (Testcontainers pulls the native one). Run `DOCKER_DEFAULT_PLATFORM=linux/arm64 docker compose up -d`, or unset the variable.

### Run the application
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)   # macOS
export OPENAI_API_KEY=sk-...                        # needed from Phase 3 onward
export APPROVAL_PASSWORD=choose-something           # needed from Phase 5: password of the approval API
mvn spring-boot:run
```
On startup Flyway applies the migrations and the two topics are created if missing. Health: `http://localhost:8080/actuator/health` (the only unauthenticated endpoint).

### Try it manually
With the app and `docker compose` running, publish a fake failed event to the DLQ and look at the quarantine table:
```bash
echo 'order-1:{"orderId": "oops' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders-dlq --property parse.key=true --property key.separator=:

docker compose exec postgres psql -U dlqagent -d dlqagent \
  -c "select correlation_id, original_topic, strategy, status, reason from quarantine_event;"
```
This calls the real model, so `OPENAI_API_KEY` must be set. A corrupt payload like this one should be quarantined with the model's rationale as the reason. Now try a repairable event, which should reappear on `orders-topic` with the audit headers:
```bash
echo 'o-1:{"orderId":"o-1","customerId":"c-1","amount":"19.99 USD","currency":"USD","status":"PAID","createdAt":"2025-04-03T10:00:00Z"}' | \
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders-dlq --property parse.key=true --property key.separator=:

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders-topic --from-beginning --property print.key=true --property print.headers=true
```
Try a Tier 2 case too: send the same event with `"amount":42.50` and `"createdAt":"03/04/2025"`. The date is ambiguous, so the model either declines (quarantined) or guesses, in which case the deterministic gate flags the repair and it waits as `PENDING_HUMAN_APPROVAL` instead of appearing on `orders-topic`. Both outcomes are safe, and which one you get varies by run. Then review it with the `curl` commands in [Human approval API](#human-approval-api), and read the trail with the audit query below:
```bash
docker compose exec postgres psql -U dlqagent -d dlqagent -c "select kafka_key, strategy, tier, outcome, planner_confidence, repair_confidence, jsonb_array_length(tool_calls) as tools, left(outcome_reason, 70) from dlq_audit_log order by occurred_at"
```

Strategy B events are quarantined for now with a "not implemented yet" reason. The commands above were run for real (real model, Kafka, Postgres and Jaeger) in the [real run](#real-run-findings); the only differences were a different Postgres port, because 5432 was taken by another project. The console producer cannot set headers, so a correlation ID is generated and the original topic defaults to `orders-topic`.

## Testing

```bash
mvn verify        # unit tests (*Test) in `test`, Testcontainers tests (*IT) in `verify`
```

Docker must be running. Tests start their own Kafka and PostgreSQL containers; they never touch `docker-compose` and never call a real LLM (the API key is a dummy value).

Current coverage (118 unit and 33 integration tests passing, SpotBugs clean; 6 live-LLM tests skipped by default):
- `InfrastructureSmokeIT`: Flyway creates both tables, both topics exist, configuration binds.
- `QuarantineFlowIT`: a corrupt event published to `orders-dlq` (with correlation ID, original-topic and stack-trace headers) appears in `quarantine_event` with all metadata; saving the same record twice writes one row; a NUL-containing payload is sanitized and stored.
- `SpringAiRemediationPlannerTest` (13 tests, scripted `ChatModel`): each strategy maps correctly; low, out-of-range and threshold-boundary confidence; unknown strategy, non-JSON and missing fields; model exceptions; JSON braces in the payload do not break the template; prompt contents, truncation and fencing; a prompt-injection payload stays inside the data fence.
- `RepairFlowIT`: Strategy A end to end with real Kafka and Postgres. A drifted event is repaired and read back from `orders-topic` with `x-repaired-by-agent`, correlation ID, summary and original headers; a hallucinated amount is rejected by the gate, quarantined and never replayed; an already-repaired event is quarantined without a loop.
- `OrderContractValidatorTest` (23 tests): every schema rule, identity and key checks, amount preservation, and the hard-violation versus risk-flag classification (ambiguous dates, filled-in and renamed fields, status and currency mapping, mechanical fixes carrying no flags).
- `PatchApplierTest` (engine only, mocked adapter) and `SchemaRepairAgentTest`: whitelist and bounds, identity fields unreachable, hallucinated amount and guessed date stopped by the gate, declined, empty and garbage proposals, model errors, non-JSON payloads and topics without an adapter never reach the model, the adapter's contract is sent per request.
- `OrderTopicAdapterTest` and `TopicAdapterRegistryTest`: identity fields are never editable, the prompt's field lists match the enforced sets, lookup by topic, duplicate adapters fail fast.
- `OrderSchemaToolsTest`: exactly two tools are exposed with the expected typed schemas; the generated tool callback works.
- `JdbcSchemaInspectorIT`: reads the real catalog; refuses non-allow-listed tables, injection strings and `pg_catalog`.
- `DlqTriageServiceTest` (19 tests, including audit content for every outcome and no audit when a replay fails): the tier policy (clean and confident goes to Tier 1, any flag goes to Tier 2 even at confidence 1.0, the lower of planner and repair confidence decides, the threshold is inclusive), unrepairable, repairer errors, replay failures propagate, spoofed topic, loop guard, planner errors, Strategy B fail-closed.
- Hardening tests: `QuarantineFlowIT` sends a 300-character key, a 100-character correlation id and a 300-character original-topic header and requires a quarantine row (not a stalled partition); `ApprovalFlowIT` proves form, `text/plain` and body-less POSTs to approve and reject are refused with 415 or 400 and change nothing; `LogSafeTest`, `FailureContextFactoryTest` (which correlation ids are trusted), `OrderContractValidatorTest` (Unicode look-alikes) and an immutability test for the records.
- `RecordingToolCallbackTest`: records name, success and duration, rethrows failures unchanged, never keeps arguments or results.
- `AuditFlowIT` (10 tests, real Kafka, Postgres and OpenTelemetry spans): audit rows for Tier 1, poison pills, Tier 2 and both human decisions (including the same `event_id` across stages and the `/api/audit` view); the table rejects `UPDATE` and `DELETE`; NUL characters in audit content are sanitized; recording twice writes one row; **a failing audit write rolls the quarantine row back**; and one trace links the Kafka consume, plan, repair, both Spring AI chat-client spans, the replay and the audit row's `trace_id`.
- `ApprovalServiceTest` and `JsonDiffTest`: only pending approvals can be decided, approve re-runs the hard checks, a failed replay leaves the item pending, reject never replays, limit clamping, field-level diff.
- `ApprovalFlowIT` (9 tests, real Kafka, Postgres and Spring Security): a risky repair is stored and not replayed; 401 for missing or wrong credentials; everything but health and the approval API is denied; the reviewer sees the diff; approve replays with `x-approved-by`, the correlation ID and the original headers and cannot be repeated; reject needs a note and never replays; a tampered proposal is refused with 409; unknown ids give 404; two simultaneous approvals replay exactly once.
- `FailureContextFactoryTest`: header extraction, generated correlation ID, topic default, tombstones, stable event ID.

### Live LLM evaluation (opt-in)
`LivePlannerEvalIT` sends realistic failures to the real configured model. Three check planner routing (date drift, a connection reset, garbage with an injection attempt). Three check the repair agent, including its real tool-calling loop, which the scripted model cannot exercise (a text amount is repaired and passes the gate; an ambiguous date is not guessed; a missing `customerId` is not invented). It costs a few cents and is skipped unless both variables are set:
```bash
DLQ_LIVE_LLM=true OPENAI_API_KEY=sk-... mvn verify -Dit.test=LivePlannerEvalIT
```
Run it again after changing provider or model to see whether routing changes.

**Results so far** (OpenAI `gpt-4o-mini`, temperature 0, 2026-10-20): all 6 scenarios passed in each of 5 consecutive runs before Phase 5, and again in 3 consecutive runs after it. With Spring AI debug logging on, the repair agent was observed calling both `describeTable` and `validateOrderPayload` against the real adapter, so the live tool-calling loop works.

**Findings from the Phase 5 live runs (3 runs, so treat as anecdotes, not rates):**
- **The gate caught a guess the model made.** For the ambiguous date `03/04/2025`, the model guessed a day/month order in 2 of 3 runs and declined in the third. In the guessing runs the deterministic gate flagged the repair, so it went to Tier 2 and was not replayed automatically. Without the gate those would have been silently replayed with a possibly wrong date.
- **The model's confidence is not a calibrated signal.** On the clean text-amount fix it reported exactly `0.90` every time. The repair prompt says to use 0.9 or more only for mechanical changes, so the model is anchoring on the number in the prompt, not measuring uncertainty. The 0.9 threshold therefore only works because the model's anchor equals it. This is why risk flags, which are deterministic, drive the decision and confidence can only demote.

**Tool-call recording** (Phase 6): after changing how tools are passed to the model (wrapped callbacks that record each call), the live evaluation passed again in 3 consecutive runs, and the recorder captured the real calls: 1 to 3 per run, all successful (`validateOrderPayload` and `describeTable`).

### Real run findings
Beyond the test suites, the actual application was run against real infrastructure: Kafka and Jaeger from `docker-compose.yml`, a fresh Postgres (all five Flyway migrations applied to an empty database), real OpenAI calls, and the README's own commands. Three events went through, one run, so these are observations, not rates:

| Event | Planner | Result |
|---|---|---|
| Truncated JSON | `POISON_PILL` | Quarantined, with the model's rationale (unclosed string) recorded in the audit row |
| Text amount `"19.99 USD"` (the clean mechanical fix) | `SCHEMA_DRIFT`, confidence **0.80** | Repair correct (diff `"19.99 USD"` → `19.99`, 3 tool calls, repair confidence 0.90, no risk flags) but routed to **Tier 2**, because the lower of planner and repair confidence (0.80) is under the 0.90 threshold. Approved through the API, replayed with `x-approved-by`, `x-repaired-by-agent`, the correlation ID and a `traceparent` header |
| Ambiguous date `03/04/2025` | `SCHEMA_DRIFT`, 0.80 | The model *declined* this time, so the event was quarantined |

**Resolved by changing the tier policy.** Originally the tier decision used the lower of the planner's and the repair's confidence. This run showed that was wrong: the planner's 0.80 (a statement about classification) kept a correct, gate-verified patch out of Tier 1. The policy now uses only the repair's confidence plus the risk flags. Re-run against the real stack afterwards: **5 of 5** text-amount events (`"11.99 USD"` to `"15.99 USD"`) went through **Tier 1** and were replayed to `orders-topic` with the correct numeric amounts and `x-repaired-by-agent: true`, each after 2 to 3 real tool calls. Caveat: in every one of those events the repair confidence was exactly `0.90`, which is the threshold. The model anchors on the number in the prompt, so this works by alignment, not by calibration. Risk flags, not confidence, remain the safety mechanism.

The Jaeger check also confirmed the exporter wiring: the four audit rows' `trace_id` values matched the trace ids Jaeger showed.

**What this does and does not show.**
- It is a smoke-level check: one scenario per category, 6 in total. It shows that the prompts, structured-output parsing, tool wiring and gate work together with a real model. It is **not** an accuracy measurement, and it says nothing about messier real-world payloads or about other models.
- The negative scenarios (garbage payload, ambiguous date, missing `customerId`) assert that the model or the gate *decided* to reject or hold the event (for the ambiguous date, either a decline or a repair carrying an `ambiguous` risk flag is accepted, since both are safe). They fail if the outcome was caused by an error. This guard exists because the first live run was wrong: a test-harness bug sent a dummy API key, every call returned 401, and the three negative scenarios "passed" only because the system fails closed on errors. The fix is in `AbstractIntegrationTest` (real key used when `DLQ_LIVE_LLM=true`) plus the `assertDecidedByTheModel` check.
- A larger labelled set (dozens of drifted, transient and poison events, scored for routing accuracy and for how often the gate rejects a proposal) would be the next step toward a defensible quality claim.

🚧 Phase 8 adds a single end-to-end suite covering all three strategies (drifted payload, transient failure, poison pill). Today the poison pill (`QuarantineFlowIT`) and drifted payload (`RepairFlowIT`) paths are tested separately.

## Configuration

Environment variables (defaults in `application.yml`):

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `DB_URL` / `DB_USER` / `DB_PASSWORD` | `jdbc:postgresql://localhost:5432/dlqagent` / `dlqagent` / `dlqagent` | PostgreSQL |
| `OPENAI_API_KEY` | `not-set` | OpenAI key |
| `OPENAI_MODEL` | `gpt-4o-mini` | OpenAI model |
| `APPROVAL_USERNAME` | `reviewer` | Username of the approval API account |
| `TRACING_EXPORT` | `false` | Set `true` to export spans over OTLP (spans are created either way) |
| `OTLP_TRACING_ENDPOINT` | `http://localhost:4318/v1/traces` | OTLP/HTTP collector, for example Jaeger |
| `APPROVAL_PASSWORD` | *(none)* | **Required.** Password of the approval API account; the app refuses to start without it |

Application settings (`dlqagent.*`, validated at startup):

| Property | Default | Meaning |
|---|---|---|
| `topics.main` / `topics.dlq` | `orders-topic` / `orders-dlq` | Topic names |
| `topics.partitions` | `3` | Partitions for declared topics |
| `retry.max-attempts` | `5` | Strategy B attempt limit |
| `retry.initial-delay` / `multiplier` / `max-delay` | `2s` / `2.0` / `5m` | Backoff curve |
| `planner.min-confidence` | `0.7` | Below this, the event is quarantined |
| `planner.max-payload-chars` | `4000` | Payload (and headers) sent to the model are truncated beyond this |
| `planner.max-stack-trace-chars` | `2000` | Stack trace sent to the model is truncated beyond this |
| `gate.auto-replay-min-confidence` | `0.9` | Tier 1 needs the repair's confidence at or above this; below it a repair waits for a human |

## Switching the LLM provider

Application code depends only on Spring AI's `ChatClient` and `ChatModel`, never on provider-specific classes. Each provider is one Maven profile that pulls one starter:

```bash
mvn spring-boot:run                  # OpenAI (default profile)
mvn spring-boot:run -Panthropic      # Claude
mvn spring-boot:run -Pollama         # local model via Ollama
```

Each also needs its own properties (API key, model name, or Ollama base URL). The planner uses only `ChatClient`, so no code changes are needed; per-provider `application-<provider>.yml` files are still to do. Behaviour differs by model: tool-calling and structured-output reliability vary, and small local models are weakest, so prompts may need tuning after a switch.

> Note: only the default `openai` profile has been built and tested so far.

## Database schema

Managed by Flyway (`src/main/resources/db/migration`).

**`quarantine_event`** (written today): events held for a human. Stores correlation ID, original topic, key, payload, headers (JSONB), stack trace, strategy and reason. `status` is one of:
- `PENDING_REVIEW`: quarantined (poison pill, hard violation, or a failure); no proposal exists.
- `PENDING_HUMAN_APPROVAL`: Tier 2. `proposed_payload`, `repair_summary` and `risk_flags` (JSONB) are filled in; a CHECK constraint guarantees a pending approval always has a proposal.
- `APPROVED_REPLAYED` / `REJECTED`: the human decision, with `reviewed_by`, `reviewed_at` and `review_notes`.
- `RESOLVED` / `DISCARDED`: reserved for manual handling of plain quarantines (no code sets these yet).

**`dlq_audit_log`**: the append-only audit trail described above (V5). Indexed by correlation ID and event ID; `UPDATE` and `DELETE` are blocked by a trigger.

**`orders`**: reference schema for the order domain, read by the schema tool (never written by this service). Its CHECK constraints mirror the order Evaluator Gate's rules and should be kept in sync.

**`retry_schedule`** (table exists, not used until Phase 7): durable Strategy B retries. Stores the event, `attempt`, `next_attempt_at` and a `status` (`SCHEDULED`, `IN_FLIGHT`, `DONE`, `EXHAUSTED`). It is Postgres-backed rather than in-memory so scheduled retries survive restarts.

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 1 | Scaffolding and infrastructure: Maven, layers, compose, Flyway, Testcontainers smoke test | ✅ Done |
| 2 | Domain model, DLQ listener, correlation IDs, quarantine persistence (Strategy C path) | ✅ Done |
| 3 | Remediation Planner (Spring AI classifier, confidence gate, prompt-injection defences; scripted-model tests, opt-in live eval) | ✅ Done (live smoke eval passes, see above) |
| 4 | Strategy A: `@Tool` schema tools, typed patch proposals, Evaluator Gate, replay, loop and topic guards | ✅ Done (live smoke eval passes, see above) |
| 4b | Topic-adapter refactor: generic engine, `OrderTopicAdapter`, strict-JSON helper. Behavior-preserving; all existing tests kept | ✅ Done |
| 5 | Two-tier gate: hard violations vs risk flags; Tier 1 auto-replay, Tier 2 `PENDING_HUMAN_APPROVAL` persisted; approve/reject API (Spring Security) | ✅ Done |
| 6 | `dlq_audit_log` (diffs, rationale, tool-call summaries, tier decision, outcome; append-only), `/api/audit`, Micrometer tracing with OTLP export | ✅ Done |
| 7 | Strategy B: health-check tool, backoff scheduler and poller; circuit breaker against replay loops | Planned |
| 8 | End-to-end suite covering all strategies and tiers, a larger labelled live evaluation set, final docs | Planned |
| 9 (idea) | **Observability tool for the agent.** A read-only `@Tool` (for example `getRecentErrorLogs(correlationId)`) behind a port, with a Jaeger or log-backend adapter, so the model can pull extra evidence about a failure instead of relying only on the stack trace in the DLQ headers. Most useful for diagnosing transient failures in Strategy B. Guardrails: read-only, scoped to one correlation ID, size-capped, and its output treated as untrusted text (log lines can carry attacker-controlled data). Needs a real log or trace backend to query | Backlog |
| 10 | **CI.** `ci.yml` (build, tests, SpotBugs) and `sca.yml` (SBOM and Grype, also daily) written and linted; **first run on GitHub still pending**. Still to do: a status badge once the first run is green, a Maven wrapper (`mvnw`), Dependabot for the pinned actions, and a license if the repo is ever made public | In progress |
