# Issue #2 implementation summary

This document maps the requested work in [plugin-clay issue #2](https://github.com/kestra-io/plugin-clay/issues/2) to the changes in this branch.

| Issue request | Implemented change |
|---|---|
| Add `io.kestra.plugin.clay.PushRows` to send rows to a Clay inbound webhook | Added a Kestra task that sends each chunk as a sequential HTTP `POST` with a JSON-array body. |
| Support optional Bearer authentication | Added optional `authToken`; when set, it is sent in the `Authorization: Bearer` header. The webhook URL and token are marked secret. |
| Accept inline rows and ION/JSON storage URIs | Resolve input using Kestra's `Data` abstraction. Tests cover inline rows and rows read from Kestra test storage. |
| Batch rows sequentially, defaulting to 100 per request | Added `chunkSize` (default `100`); chunks preserve input order. |
| Observe the Clay 50,000-row-per-source limit | Reject more than 50,000 rows in one task execution before sending a request. Documentation notes that prior submissions to the webhook cannot be determined by this task. |
| Retry HTTP `429` and `5xx` responses with exponential backoff | Added retries using Kestra's retry utility: three total attempts, one-second initial interval, and a 10-second cap. Tests cover `429` and `5xx` followed by success. |
| Handle chunk failures in fail-fast or best-effort mode | Added `failOnPartialError` (default `true`). In best-effort mode, failed zero-based chunk indices are recorded and later chunks continue. |
| Support Kestra expressions and return execution outputs | Task properties are rendered through `RunContext`. Outputs include `rowCount`, `chunkCount`, and `failedChunks`. |
| Do not add a dedicated Clay callback trigger; document Kestra's generic webhook pattern | No Clay-specific trigger was added; the README points callback use cases to `io.kestra.plugin.core.trigger.Webhook`. |
| Add plugin metadata, category, icon, examples, docs, and tests | Added the `BUSINESS` category, plugin metadata and icon, task examples/documentation, and automated tests. |

## Reviewer note

The issue describes `rowCount` as total rows submitted and `chunkCount` as the number of POST requests. This implementation reports rows in successful chunks and logical chunks; retry attempts do not increase `chunkCount`. I’d like to confirm this interpretation with the maintainer during review.

## Verification

- `./gradlew test` passes, including the Kestra storage URI test.
- The local Kestra UI loads `PushRows` and displays its generated task form.
- A live Clay request was not run because no Clay webhook URL was available.
