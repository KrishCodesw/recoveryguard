# Engineering and Contribution Standard

This document defines the contribution process for the RecoveryGuard product repository.

## 1. Change control

- Do not push directly to `main`.
- Create a short-lived branch from the current `main`.
- Keep each pull request focused and reviewable.
- Explain the problem, design choice, affected components, and risk.
- Include test commands and their actual results.
- Update affected documentation.
- Wait for CI and maintainer review.
- Do not merge without explicit maintainer approval.

Suggested branch prefixes: `docs/`, `observer/`, `evidence/`, `engine/`, `persistence/`, and `build/`.

## 2. Product contracts

Controlled contracts include evidence schemas, event names, verdict values, check semantics, rule precedence, report formats, and compatibility claims.

Change them only through a reviewed pull request. Do not silently rename fields, enum values, or statuses. Keep code, tests, decision tables, and documentation aligned.

Rules marked `OPEN` are unresolved. Do not assume an answer.

## 3. Product and Lab separation

The product repository must not import, depend on, package, or execute RecoveryGuard Lab code.

The Lab may use a pinned copy of an approved product contract where needed for comparison. It must not use product implementation classes as its ground-truth oracle.

Ground truth must be established independently.

## 4. Observer safety

The Observer uses Kafka Admin APIs only. It must not create consumers, subscribe or assign partitions, poll records, join groups, commit offsets, produce records, create or delete topics, change configuration, change assignments, or trigger recovery.

Every new Admin API operation must document its purpose, required Kafka permissions, request cost, timeout, retry behaviour, partial-failure behaviour, and tests.

## 5. Evidence and time

- Preserve raw observations.
- Do not overwrite or silently repair evidence.
- Keep source observation time separate from collection and processing time.
- Do not infer causality from display order.
- Mark missing, partial, stale, or uncertain evidence.
- Do not treat a hash check as proof of source authenticity.
- Do not treat Kafka metadata as record-level proof.

## 6. VerdictEngine requirements

VerdictEngine must remain a pure function:

- no Kafka client calls;
- no database or filesystem access;
- no clock reads;
- no environment or global mutable state;
- no logging or network calls;
- all decision inputs passed as arguments; and
- same input produces the same result or the same explicit OPEN-rule exception.

Decision-table and property tests must cover approved rules.

## 7. Java and Maven

- Target Java 17.
- Use Maven for builds and tests.
- Pin dependency and plugin versions.
- Keep dependencies minimal.
- Prefer typed interfaces and explicit behaviour.

Before opening a pull request, run:

```bash
mvn --batch-mode --no-transfer-progress clean verify
```

Run the Lab-backed integration workflow when the change affects Observer integration, evidence capture, or compatibility. Report exact commands and actual results.

## 8. Documentation standard

Documentation is part of the product interface.

- Use exact names from code and configuration.
- Separate current behaviour from target architecture.
- Mark proposals, limitations, and OPEN decisions.
- Include prerequisites and tested commands for operational procedures.
- State permissions and safety impact for Kafka operations.
- Do not claim Kafka compatibility without test evidence.
- Do not use marketing language in place of technical evidence.

## 9. Pull request checklist

- [ ] Scope and design reason are clear.
- [ ] No direct change to `main`.
- [ ] Product/Lab separation is preserved.
- [ ] Observer safety boundary is preserved.
- [ ] Contracts and OPEN decisions are handled correctly.
- [ ] Tests cover changed behaviour.
- [ ] Documentation is updated.
- [ ] Test results are accurate.
- [ ] CI passes.
- [ ] Maintainer approval is recorded.
