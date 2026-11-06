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

## Build

Requirements: JDK 17 and Maven 3.9 or later.

```bash
mvn --batch-mode --no-transfer-progress test
```

Cluster metadata:

```bash
mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=com.recoveryguard.observer.ClusterInfoMain \
  '-Dexec.args=--bootstrap-server localhost:29092'
```

Topic and partition metadata:

```bash
mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=com.recoveryguard.observer.TopicPartitionInfoMain \
  '-Dexec.args=--bootstrap-server localhost:29092'
```

Topic configuration metadata:

```bash
mvn --batch-mode --no-transfer-progress exec:java \
  -Dexec.mainClass=com.recoveryguard.observer.TopicConfigInfoMain \
  '-Dexec.args=--bootstrap-server localhost:29092'
```

See [the observer guide](docs/observer.md) for output, security settings, and limits.

## Development workflow

1. Create a feature branch from `main`.
2. Make one focused change.
3. Run the relevant build and tests locally.
4. Open a pull request.
5. Wait for CI and review.
6. Merge only after explicit maintainer approval.

Do not push directly to `main`.

## Related repository

`recoveryguard-lab` contains the independent Kafka test environment and, in later stages, the fault-injection and ground-truth tools. It is not a product build or runtime dependency. The observer integration workflow checks out one fixed Lab commit as a test fixture. The Lab may consume only a pinned, approved copy of the verdict contract/schema. RecoveryGuard must not import Lab code, packages, images, or build artifacts.

## Documentation

Start with [the architecture](docs/architecture.md), then read the [glossary](docs/glossary.md) and [contribution rules](docs/contributing.md).
