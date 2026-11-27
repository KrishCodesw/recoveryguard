# Security Policy

## Scope

RecoveryGuard connects to Kafka clusters and stores operational evidence. Treat Kafka metadata, cluster identifiers, topic names, configuration values, investigation reports, and evidence databases as customer-sensitive information.

The current codebase is under active development. Do not assume that it is suitable for production use.

## Reporting a vulnerability

Do not publish credentials, customer data, exploit details, or an unpatched vulnerability in a public issue.

Use GitHub's private vulnerability reporting feature for this repository when it is enabled. If private reporting is not available, contact the repository maintainers through a private channel before you disclose the issue publicly.

Include:

- affected repository and commit;
- affected component and configuration;
- impact and prerequisites;
- steps to reproduce, where safe;
- relevant logs with secrets and customer data removed; and
- a suggested mitigation, if known.

## Security design requirements

- Use least-privilege Kafka identities.
- Keep credentials out of source control, command output, and logs.
- Use TLS and approved authentication for non-local deployments.
- Keep evidence storage outside the monitored Kafka cluster.
- Restrict access to evidence databases and reports.
- Preserve the Observer's read-only operation boundary.
- Treat evidence hashes as integrity checks, not source authentication.
- Do not expose local plaintext Lab listeners to untrusted networks.

## Supported release status

RecoveryGuard is not currently declared production-ready. Security controls, supported deployment modes, and vulnerability response commitments must be reviewed before a production release.
