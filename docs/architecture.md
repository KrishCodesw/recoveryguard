# RecoveryGuard Product Architecture

**Status:** Architecture direction for early implementation. Detailed interfaces and schema fields remain subject to approved design decisions.

## 1. Purpose

RecoveryGuard connects to an existing Kafka deployment, observes relevant cluster state, preserves evidence before and after a recovery event, evaluates explicit correctness checks, and produces an explainable report.

Its product position is:

> A post-incident evidence bundle with an explicit verdict on whether the observed Kafka recovery satisfied the configured correctness checks.

RecoveryGuard complements Kafka operations, monitoring, balancing tools, and runbooks. It does not replace them and does not perform recovery actions.

## 2. Product boundaries

- No broker plugin or broker-side agent is required.
- No customer topic or broker configuration is changed by the observer.
- No application code change is required for Kafka-level validation.
- The observer uses Kafka Admin APIs only.
- The observer must not create or use Kafka consumers, subscribe to topics, join groups, or commit offsets.
- Evidence storage must be independent of the monitored Kafka cluster.
- Application correctness is evaluated only when the customer supplies a defined contract or independent evidence source.
- The reference workload and fault harness live in `recoveryguard-lab`; the product must not depend on that repository.

## 3. Logical architecture

The target product has two deployment roles. The first implementation may combine several logical services in one process. Do not split modules into microservices without an explicit need.

### RecoveryGuard Site

The Site is the control, evidence, validation, and reporting boundary. Its logical modules are:

- **API and Console:** cluster registration, access control, incident views, and report access.
- **Cluster Registry:** connection profile, endpoint metadata, enabled capabilities, and secret references.
- **Scheduler / Incident Manager:** collection schedules and incident windows. Initial development may use manual incident windows.
- **Evidence Ingestion:** validates incoming evidence envelopes and records provenance.
- **Evidence Catalog:** indexes evidence and links it to clusters, collection runs, and incidents.
- **Validation Engine:** evaluates versioned checks against an evidence set. The engine is independent of Kafka client code.
- **Rule Registry:** records rule IDs, versions, applicability, required/optional status, and evidence requirements.
- **Report Generator:** creates machine-readable and human-readable reports.

### RecoveryGuard Agent

The Agent runs where it can reach the customer Kafka endpoints. It:

- loads a cluster connection profile and secret references;
- creates a Kafka Admin client;
- discovers broker and API capabilities;
- collects bounded, timestamped metadata snapshots;
- applies request budgets and rate controls;
- records partial collection and failure states;
- persists a local spool when the Site is unavailable; and
- sends evidence to the Site over an authenticated channel.

The Agent must not contain verdict policy. It collects evidence; the Site validates it.

## 4. Customer deployment and connectivity

The intended deployment is self-hosted inside the customer's network or cloud account. It may run on Kubernetes, a VM, or a supported container host.

The Agent must be able to resolve and reach every broker endpoint returned by Kafka metadata, not only the bootstrap address. The connection test must therefore verify advertised broker endpoints.

Connection profiles should store:

- cluster display name and environment;
- bootstrap server list;
- security protocol;
- SASL mechanism or TLS client identity, where applicable;
- reference to a secret or certificate source;
- trust configuration;
- optional topic and consumer-group observation scope; and
- configured request and polling limits.

Credentials must not be stored in plain-text connection profiles or logs. Provider-specific authentication belongs in connector modules, not in the validation engine.

## 5. Observation and evidence flow

1. An administrator registers a Kafka cluster and provides a least-privilege identity.
2. The Agent tests connectivity, authentication, permissions, and advertised broker reachability.
3. The Agent discovers supported capabilities and records unsupported or unavailable APIs.
4. The Agent collects timestamped baseline snapshots.
5. Evidence is transferred to storage outside the monitored Kafka cluster.
6. An incident window is opened manually in the initial release. Later versions may detect incidents from configured signals.
7. The Agent collects post-event and recovery snapshots.
8. Evidence quality checks evaluate freshness, completeness, clock uncertainty, and source availability.
9. The Validation Engine evaluates applicable checks.
10. RecoveryGuard emits a verdict and evidence report.

A snapshot is a point-in-time observation, not a continuous record of every broker event. Collection gaps must be visible. RecoveryGuard must not infer an unobserved transition.

## 6. Kafka observation boundary

The observer uses the Kafka Admin API.

The first implementation slice currently calls only `Admin.describeCluster()`. It records the cluster ID, controller node, and broker endpoints. It does not yet list topics, inspect offsets or consumer groups, or collect partition and ISR state. A successful cluster description is not a recovery verdict.

Candidate read operations for later, separately reviewed slices include:

- cluster description;
- topic listing and topic description;
- topic configuration description;
- list offsets for configured topic partitions;
- consumer-group description; and
- committed group-offset inspection.

The exact API allowlist and supported broker-version behaviour must be tested and recorded. Group metadata inspection is not the same as joining a consumer group.

Forbidden observer behaviour:

- constructing a Kafka Consumer;
- subscribing or assigning partitions to a consumer;
- polling records;
- joining or triggering consumer-group membership;
- committing offsets;
- producing records;
- creating or deleting topics;
- changing topic or broker configuration;
- changing partition assignments or electing leaders.

Read-only intent alone is not enough. The observer must use request budgets, bounded concurrency, timeouts, backoff, and configurable polling intervals. Tests must compare target-cluster behaviour with and without observation.

## 7. Evidence storage

Evidence must not be stored only in the Kafka cluster being observed.

The target Site storage model is:

- **Relational metadata store:** cluster profiles, collection runs, incident windows, evidence indexes, rule versions, check results, users, and audit events.
- **Object storage:** raw snapshots, normalized evidence payloads, manifests, and report bundles.
- **Agent local spool:** bounded durable buffering during Site or network outages.

Each evidence item should identify its source, operation, observation time, collection time, schema version, Agent version, cluster identity, payload reference, integrity hash, and collection status.

Retention, encryption, export, and deletion policies must be configurable. Evidence hashes provide integrity checks; they do not by themselves prove source authenticity.

For the Lab and first local experiments, a small local store may be used. That does not define the target production storage design.

## 8. Validation model

Validation is split into distinct scopes:

- **Kafka-level checks:** cluster and partition state, leader and ISR metadata, offsets, configuration, and observed recovery transitions.
- **Platform checks:** optional Kubernetes or infrastructure evidence such as broker pod restarts and node events.
- **Application-contract checks:** optional customer-provided event identities, acknowledgement records, causal relationships, and business invariants.
- **Evidence-quality checks:** freshness, completeness, provenance, clock uncertainty, and collection gaps.

Every check must declare its ID and version, applicability, required/optional classification, required evidence, result, explanation, evidence references, and limitations.

Kafka metadata alone cannot prove that every acknowledged business event survived. Offset gaps may have valid causes such as retention, compaction, or transaction control records. Claims about application data preservation require a suitable pre-failure baseline and independent application evidence.

The final verdict policy belongs to the approved verdict contract. Do not invent or infer rules that are marked OPEN.

## 9. Verdict and report

The report must include:

- cluster identity and validation scope;
- incident window and timestamps;
- Kafka, Agent, and rule versions;
- evidence sources and collection health;
- baseline and post-recovery evidence references;
- every required and optional check result;
- final verdict and confidence basis;
- limitations, unsupported capabilities, and missing evidence; and
- hashes or references for the evidence bundle.

A validated verdict means that the configured required checks passed with sufficient evidence. It is not a universal guarantee that the customer's application data is correct.

## 10. Security

- Use a dedicated least-privilege Kafka identity.
- Use TLS for Kafka connections and authenticated encryption for Agent-to-Site communication.
- Store credential references, not secret values, in configuration.
- Support customer-controlled secret sources.
- Keep the observer operation allowlist in code and test it.
- Apply RBAC to Site users and record administrative actions.
- Encrypt stored evidence and protect signing keys.
- Avoid collecting record payloads by default.
- Treat topic names, group names, broker metadata, and configuration as customer-sensitive data.
- Run containers as non-root with minimum capabilities.
- Do not expose Kafka brokers or the Site API to the public Internet by default.

## 11. Failure handling

The product must record collection gaps rather than hide them.

Expected behaviour includes:

- bounded retries and backoff for transient Kafka failures;
- clear authentication and authorization errors;
- partial evidence when only some APIs or brokers are reachable;
- local durable buffering during Site outages;
- explicit spool-capacity alerts;
- upload retries with idempotent evidence identifiers;
- clock-skew and timestamp-quality warnings;
- explicit unsupported-capability results; and
- no validated verdict when required evidence is missing.

A failure of RecoveryGuard itself must not be reported as a Kafka recovery failure.

## 12. Upgrades and compatibility

Version independently:

- Agent release;
- evidence schema;
- validation rule pack; and
- report schema.

Historical reports must retain the versions used to produce them. Schema migrations must be explicit. Upgrades need health checks and a rollback path.

Publish a tested compatibility matrix by Kafka distribution, broker version, authentication mode, and enabled evidence capability. Do not claim support for a target that has not passed integration tests.

## 13. Deployment phases

The first vertical slice is intentionally smaller than the target architecture:

1. single local Site/collector process;
2. Kafka Admin API snapshot poller;
3. bounded local evidence storage;
4. manually selected incident window;
5. a small, approved set of Kafka-level checks;
6. verdict and JSON report; and
7. independent Lab ground-truth comparison.

High availability, continuous incident detection, provider-specific adapters, application contracts, and enterprise operations are later capabilities. Their interfaces should not be blocked by the first slice, but they must not be claimed as implemented.
