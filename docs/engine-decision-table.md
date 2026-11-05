# Verdict Engine Decision Table

**Status:** Proposed implementation for review. This is not the approved external verdict schema. Contract and verdict-rule changes require project-owner approval.

## Inputs

A decision is calculated from an immutable list of `CheckResult` values.

- `Kind` describes why a check exists: `INTEGRITY_PROOF`, `EVIDENCE_GATE`, or `ADVISORY`.
- `required` states whether the check is required for a positive verdict.
- `Outcome` is the check's execution result.
- A proven violation must be an `INTEGRITY_PROOF` result with at least one evidence reference.
- `ADVISORY` checks must be optional.
- `WARN` is reserved for optional advisory checks.

## Implemented decision table (proposal)

| Condition | Proposed result |
|---|---|
| Any check proves an integrity violation | `DATA_INTEGRITY_FAILURE`, including an optional integrity check |
| No proven violation; required check has `ERROR`; no required incomplete check | `VALIDATION_ERROR` |
| No proven violation or required error; required check is `INCONCLUSIVE` or `NOT_RUN` | `INCONCLUSIVE` |
| All required checks pass and at least one required integrity-proof check exists | `RECOVERY_VALIDATED` |
| Optional advisory check is `WARN`, `ERROR`, `INCONCLUSIVE`, or `NOT_RUN` | Does not block a positive verdict |

A proven violation is evaluated before other check outcomes. Therefore, a later check error cannot hide an already proven violation.

The engine is a pure function. It does not access Kafka, storage, the clock, the network, logging, or environment variables. It derives all counts and check-ID lists from its input.

## OPEN rules — do not build on these

The implementation fails fast when it reaches an OPEN rule. These exceptions are development guards, not RecoveryGuard verdicts.

| ID | OPEN question | Current handling |
|---|---|---|
| `OPEN-01_ERROR_VS_INCONCLUSIVE_PRECEDENCE` | When required checks contain both `ERROR` and `INCONCLUSIVE` or `NOT_RUN`, which outcome takes precedence if no violation was proven? | `OpenDecisionException`; no verdict is chosen. |
| `OPEN-02_EMPTY_CHECK_LIST` | What verdict, if any, applies when the engine receives no checks? | `OpenDecisionException`; no verdict is chosen. |
| `OPEN-03_REQUIRED_INTEGRITY_PROOF_GATE` | Must a positive verdict require at least one required `INTEGRITY_PROOF` check, or can other required checks alone support it? | `OpenDecisionException` when all required checks pass without a required integrity-proof check. |

OPEN-03 is proposed as an unresolved gate. Its presence in this PR does not approve the requirement.

## Input validation

The engine rejects malformed input:

- null check list or null check item;
- blank or duplicate check IDs;
- null kind or outcome;
- required advisory checks;
- `WARN` on a non-advisory or required check;
- `VIOLATION_PROVEN` on a non-integrity check; and
- a proven violation without an evidence reference.

These are input-integrity constraints, not verdict precedence rules.

## Tests

- Decision-table tests cover each closed case and each OPEN guard.
- Property tests cover violation dominance, input-order invariance, optional advisory non-interference, and exhaustive combinations of the closed required-check outcomes.
- No external evidence schema is added or changed in this PR.
