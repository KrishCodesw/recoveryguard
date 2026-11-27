# Kafka Compatibility

**Status:** Compatibility claims are limited to configurations exercised by integration tests.

## 1. Policy

RecoveryGuard does not claim support for a Kafka distribution or broker version because the Kafka client library can connect to it. Support requires integration tests for the Admin API operations used by the Observer.

A compatibility result must identify:

- Kafka distribution and broker version;
- Kafka client version;
- topology and controller mode;
- authentication and transport security;
- Admin API operations tested;
- test date and product commit; and
- test result, including known limitations.

## 2. Current evidence

| Target | Evidence | Status |
|---|---|---|
| Apache Kafka 4.4.0, three combined broker/controller nodes, KRaft, PLAINTEXT | Lab smoke test and Observer integration workflow | Tested in the current CI profile |
| Other Apache Kafka versions | No compatibility result recorded here | Not yet verified |
| Strimzi-managed Kafka | No Kubernetes/Strimzi integration profile recorded here | Not yet verified |
| TLS or SASL deployment profiles | Client properties are supported by the command interface; no complete compatibility matrix is recorded here | Not yet verified |
| Dedicated controller topology | Not represented by the current product integration profile | Not yet verified |

## 3. Interpretation

The Kafka 4.4.0 result applies to the exact Lab profile used by CI. It does not establish compatibility with every Kafka 4.4 deployment or configuration.

A successful connection test is not a complete compatibility test. Each supported target must exercise the required metadata operations and failure paths.

## 4. Adding a compatibility target

1. Define the target Kafka distribution, version, topology, and security mode.
2. Identify every Admin API operation required by the product.
3. Add an isolated integration profile.
4. Test successful, partial, unauthorized, and timed-out requests.
5. Record the exact product and Lab commits.
6. Update this matrix only after the test result is available.
