# Kestra Clay Plugin

## What

- Provides plugin components under `io.kestra.plugin.clay`.
- Provides the `PushRows` task, which sends records to a Clay table through its inbound webhook in sequential, retried chunks.

## Why

- What user problem does this solve? Teams need to feed rows from flows (database queries, files, API results) into Clay tables for enrichment without writing custom webhook code.
- Why would a team adopt this plugin in a workflow? `PushRows` handles chunking, retries on network errors, HTTP 429 and 5xx, and bearer authentication.
- What operational/business outcome does it enable? Reliable, observable Clay table ingestion from Kestra, with explicit partial-failure handling.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `clay`

Infrastructure dependencies (Docker Compose services):

- `app`

### Key Plugin Classes

- `io.kestra.plugin.clay.PushRows`

### Project Structure

```
plugin-clay/
├── src/main/java/io/kestra/plugin/clay/
├── src/test/java/io/kestra/plugin/clay/
├── build.gradle
└── README.md
```

## Local rules

- Base the wording on the implemented packages and classes, not on template README text.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
