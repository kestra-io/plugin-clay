# Clay plugin

The Clay plugin sends records from Kestra workflows to a Clay table using its inbound webhook URL.

## Setup

In Clay, create a table source by selecting **Add** and then **Monitor webhook**. Copy the generated URL. If you enable authentication, copy the token when Clay displays it; Clay only shows it once. Store both values in Kestra secrets.

Clay webhook sources have a limit of 50,000 submissions. `PushRows` rejects more than 50,000 rows in one task execution, but it cannot determine how many rows have already been submitted to that webhook.

## Push rows

`io.kestra.plugin.clay.PushRows` posts a JSON array of records to the configured Clay webhook. It accepts inline rows or a Kestra internal-storage URI containing JSON or ION records. Rows are sent sequentially in chunks of 100 by default.

```yaml
id: push_leads_to_clay
namespace: company.growth

tasks:
  - id: push_to_clay
    type: io.kestra.plugin.clay.PushRows
    webhookUrl: "{{ secret('CLAY_WEBHOOK_URL') }}"
    authToken: "{{ secret('CLAY_WEBHOOK_TOKEN') }}"
    rows:
      - email: alice@example.com
        company: Acme
      - email: bob@example.com
        company: Globex
    chunkSize: 100
    failOnPartialError: true
```

Set `rows` to a previous task's records or file URI to pass query or export results. Use `chunkSize` to control the number of rows in each POST request.

The task retries HTTP `429` and `5xx` responses up to three total attempts using Kestra's exponential retry policy. Other HTTP errors fail without retry. With `failOnPartialError: true` (the default), the task stops at the first chunk that still fails after retries. Set it to `false` to record failed chunks and continue sending later chunks.

The output contains `rowCount` (rows in successful chunks), `chunkCount` (logical chunks processed), and `failedChunks` (zero-based indices of failed chunks). Retries do not increase the chunk count.

Retries and rerunning a task can result in duplicate submissions if Clay accepted a request but Kestra did not receive the response. Clay's webhook documentation does not specify an idempotency key for these requests.

## Receive Clay callbacks

To receive data sent outward from a Clay enrichment action, use Kestra's generic webhook trigger. The callback payload is configured in Clay and does not have a standardized Clay-specific envelope, so this plugin does not provide a dedicated trigger.

See the [Kestra Webhook trigger documentation](https://kestra.io/plugins/plugin-core/triggers/io.kestra.plugin.core.trigger.webhook).

## References

- [Clay webhook documentation](https://university.clay.com/docs/webhook-integration-guide)
- [Kestra plugin developer guide](https://kestra.io/docs/plugin-developer-guide)
