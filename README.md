# RecoveryGuard

RecoveryGuard is an independent Kafka recovery-validation system.

It observes a Kafka deployment, records evidence around a recovery event, evaluates explicit correctness checks, and produces an explainable verdict with evidence references and limitations.

> Project status: early development. This repository is a product codebase, not a production-ready release.

## Product boundaries

- RecoveryGuard connects to customer Kafka clusters through the Kafka Admin API.
- The observer must not create Kafka consumers, join consumer groups, subscribe to topics, or commit offsets.
- The product does not require Kafka broker plugins or changes to customer applications.
- Evidence must be stored outside the monitored Kafka cluster.
- Kafka-level checks and application-level checks are separate.
- Application-level validation requires an explicit customer-provided contract or evidence source. Kafka metadata alone must not be used to claim business correctness.
- The product repository must never import or depend on `recoveryguard-lab`.

## Repository layout

The structure will grow through small, reviewed pull requests.

- `docs/architecture.md` — product boundaries, logical components, deployment and data flow.
- `docs/glossary.md` — shared technical terminology.
- `docs/contributing.md` — contribution, review and testing rules.
- `pom.xml` — Java 17 Maven project.
- `.github/workflows/ci.yml` — pull-request build and test workflow.

## Build

Requirements:

- JDK 17
- Maven 3.9 or later

Run:

```bash
mvn --batch-mode --no-transfer-progress test
```

The current foundation PR adds the Maven build and CI workflow. Product modules and tests will be added in later PRs.

## Development workflow

1. Create a feature branch from `main`.
2. Make one focused change.
3. Run the relevant build and tests locally.
4. Open a pull request.
5. Wait for CI and review.
6. Merge only after explicit maintainer approval.

Do not push directly to `main`.

## Related repository

`recoveryguard-lab` contains the reference workload, fault-injection environment, and independent ground-truth tools. It is not a product dependency. The Lab may consume only a pinned, approved copy of the verdict contract/schema. RecoveryGuard must not import Lab code, packages, images, or build artifacts.

## Documentation

Start with [the architecture](docs/architecture.md), then read the [glossary](docs/glossary.md) and [contribution rules](docs/contributing.md).
