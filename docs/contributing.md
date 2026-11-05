# Contributing to RecoveryGuard

## 1. Branch and pull-request workflow

- Never push directly to `main`.
- Create a short-lived feature branch from `main`.
- Keep each pull request focused on one small change.
- Include a clear problem statement, design notes, and test evidence.
- Wait for CI and maintainer review.
- Do not merge a pull request without explicit approval from the project owner.

Suggested branch names:

- `docs/<short-description>`
- `engine/<short-description>`
- `observer/<short-description>`
- `build/<short-description>`

## 2. Contract and verdict ownership

Evidence schemas, verdict schemas, verdict rules, and check semantics are product contracts.

- Change them only through a pull request.
- The project owner (Krish Jain) must approve contract and verdict-rule changes.
- Do not silently rename fields, enum values, statuses, or precedence.
- Do not build on any item marked `OPEN`.
- Keep the decision record and tests aligned with every approved change.

## 3. Product and Lab separation

The product repository must never import, depend, package, or execute code from `recoveryguard-lab`.

The Lab may use only a pinned copy of an approved verdict schema/contract. It must not depend on product implementation classes or reuse the product's validator as its ground-truth oracle.

Ground truth must be established independently.

## 4. Observer safety

The observer uses Kafka Admin APIs only.

Prohibited behaviour:

- creating a Kafka Consumer;
- subscribing or assigning partitions to a consumer;
- polling topic records;
- joining consumer groups;
- committing offsets;
- producing records; or
- changing cluster, topic, partition, or broker state.

Every new Admin operation must state why it is needed, what permission it requires, its request cost, and how it is bounded.

Tests must verify the observer's allowed API boundary.

## 5. Verdict engine

The VerdictEngine must be a pure function.

- No Kafka client calls.
- No filesystem or database access.
- No system clock reads.
- No environment or global mutable state.
- No logging or network calls.
- All decision inputs must be passed as arguments.
- The same input must always produce the same output.

Decision-table and property tests must cover the approved rules. OPEN rules must remain explicit and must not be guessed.

## 6. Java and Maven

- Target Java 17.
- Use Maven for builds and tests.
- Keep dependencies minimal.
- Pin plugin and dependency versions.
- Do not add a dependency without a clear reason.
- Avoid reflection or runtime magic where a simple typed interface is sufficient.

Before opening a PR, run:

```bash
mvn --batch-mode --no-transfer-progress clean test
```

Report the exact command and result. Do not claim a test passed unless it ran.

## 7. Documentation

Update documentation when behaviour, configuration, permissions, compatibility, or a contract changes. Mark proposals and OPEN decisions clearly. Do not describe planned functionality as implemented.

## 8. Pull-request checklist

- [ ] One focused change.
- [ ] No direct change to `main`.
- [ ] Product/Lab boundary preserved.
- [ ] Observer safety preserved.
- [ ] Contract changes have project-owner approval.
- [ ] OPEN decisions remain unresolved unless approved.
- [ ] Tests added or updated.
- [ ] Local test results reported accurately.
- [ ] CI passes.
- [ ] Documentation updated where needed.
