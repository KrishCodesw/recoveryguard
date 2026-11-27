# Verdict Engine Decision Table

**Component:** `VerdictEngine`  
**Status:** Implemented prototype; decision contract is not fully approved

## 1. Purpose

The VerdictEngine is a pure Java decision function. It evaluates an immutable list of `CheckResult` values and returns a verdict when the applicable rules are defined.

It does not connect to Kafka, read evidence, access storage, inspect the clock, log, or use network services. Check execution and evidence validation are upstream responsibilities.

## 2. Verdict values in code

| Value | Current meaning |
|---|---|
| `RECOVERY_VALIDATED` | All required checks passed and the current required integrity-proof gate was met |
| `DATA_INTEGRITY_FAILURE` | At least one check proves an integrity violation |
| `INCONCLUSIVE` | A required check is inconclusive or was not run, with no proven violation or required error |
| `VALIDATION_ERROR` | A required check returned an error, with no proven violation or required incomplete check |

These are internal enum values. They are not yet a published external API or report schema.

## 3. Input model

A decision uses a list of `CheckResult` values:

- `checkId` identifies the check.
- `Kind` is `INTEGRITY_PROOF`, `EVIDENCE_GATE`, or `ADVISORY`.
- `required` indicates whether the check is required for a positive result.
- `Outcome` records the check execution result.
- A proven integrity violation requires at least one evidence reference.
- Limitations are carried into the result.

Input constraints include non-null values, unique non-blank check IDs, optional-only advisory checks, `WARN` only for optional advisory checks, and `VIOLATION_PROVEN` only for an integrity-proof check with evidence.

## 4. Implemented decision behaviour

| Condition | Current behaviour |
|---|---|
| Any check proves an integrity violation | Return `DATA_INTEGRITY_FAILURE`, including an optional integrity check |
| No proven violation; a required check has `ERROR`; no required incomplete check | Return `VALIDATION_ERROR` |
| No proven violation or required error; a required check is `INCONCLUSIVE` or `NOT_RUN` | Return `INCONCLUSIVE` |
| All required checks pass and at least one required integrity-proof check exists | Return `RECOVERY_VALIDATED` |
| Optional advisory check returns `WARN`, `ERROR`, `INCONCLUSIVE`, or `NOT_RUN` | Does not block a positive verdict by itself |

A proven violation is evaluated before other outcomes. A later check error cannot hide a proven violation.

## 5. OPEN decisions

The engine fails fast when an unresolved rule affects the result. These exceptions are development guards, not product verdicts.

| ID | Unresolved question | Current behaviour |
|---|---|---|
| `OPEN-01_ERROR_VS_INCONCLUSIVE_PRECEDENCE` | If required checks contain both `ERROR` and `INCONCLUSIVE` or `NOT_RUN`, which result takes precedence when no violation is proven? | Throw `OpenDecisionException` |
| `OPEN-02_EMPTY_CHECK_LIST` | What result applies when the engine receives no checks? | Throw `OpenDecisionException` |
| `OPEN-03_REQUIRED_INTEGRITY_PROOF_GATE` | Must a positive verdict require at least one required `INTEGRITY_PROOF` check? | Throw `OpenDecisionException` when all required checks pass without a required integrity-proof check |

Do not build downstream behaviour that assumes an answer to an OPEN decision. Each decision requires review, an approved contract update, and tests.

## 6. Result contents

The current `VerdictResult` contains the verdict, required-check counts, proven-violation check IDs, error check IDs, inconclusive or not-run check IDs, and limitations.

This is an internal Java model. It is not a stable external schema.

## 7. Tests and change control

Tests cover decision-table cases, violation dominance, input-order invariance, optional advisory non-interference, and OPEN-rule guards.

Verdict values, check semantics, rule precedence, and evidence requirements are controlled product contracts. Changes require a reviewed pull request and explicit project-owner approval.
