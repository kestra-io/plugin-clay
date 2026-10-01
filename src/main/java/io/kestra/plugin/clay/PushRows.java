package io.kestra.plugin.clay;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.http.client.configurations.TimeoutConfiguration;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.RetryUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Push rows to a Clay table",
    description = """
        Sends records to a Clay table through its inbound webhook, in sequential batches. \
        Network errors, HTTP 429 and HTTP 5xx responses are retried up to 3 times with exponential backoff."""
)
@Plugin(
    examples = {
        @Example(
            title = "Send rows to a Clay table",
            full = true,
            code = """
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
                """
        ),
        @Example(
            title = "Send rows produced by another task",
            full = true,
            code = """
                id: push_query_results_to_clay
                namespace: company.growth

                tasks:
                  - id: query_contacts
                    type: io.kestra.plugin.jdbc.postgresql.Query
                    url: "jdbc:postgresql://localhost:5432/crm"
                    username: "{{ secret('PG_USER') }}"
                    password: "{{ secret('PG_PASSWORD') }}"
                    sql: "SELECT email, first_name, company FROM contacts WHERE enriched_at IS NULL"
                    fetchType: FETCH

                  - id: push_to_clay
                    type: io.kestra.plugin.clay.PushRows
                    webhookUrl: "{{ secret('CLAY_WEBHOOK_URL') }}"
                    rows: "{{ outputs.query_contacts.rows }}"
                    chunkSize: 100
                """
        )
    }
)
public class PushRows extends Task implements RunnableTask<PushRows.Output> {
    private static final int MAX_ROWS_PER_EXECUTION = 50_000;
    private static final int MAX_CHUNK_SIZE = 5_000;
    private static final int ERROR_BODY_EXCERPT_LENGTH = 200;
    private static final long MAX_RETRY_AFTER_SECONDS = 30;
    private static final Exponential RETRY_POLICY = Exponential.builder()
        .interval(Duration.ofSeconds(1))
        .maxInterval(Duration.ofSeconds(10))
        .delayFactor(2.0)
        .maxAttempts(3)
        .build();
    private static final TimeoutConfiguration TIMEOUT = TimeoutConfiguration.builder()
        .connectTimeout(Property.ofValue(Duration.ofSeconds(10)))
        .readIdleTimeout(Property.ofValue(Duration.ofSeconds(30)))
        .build();

    @Schema(
        title = "Clay webhook URL",
        description = """
            Absolute HTTP(S) URL generated for the Clay table webhook. Store it as a Kestra secret."""
    )
    @NotNull
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @PluginProperty(group = "connection", secret = true)
    private Property<String> webhookUrl;

    @Schema(
        title = "Clay webhook authentication token",
        description = """
            Optional token sent as a Bearer authorization header. Store it as a Kestra secret."""
    )
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @PluginProperty(group = "connection", secret = true)
    private Property<String> authToken;

    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION,
        anyOf = { String.class, List.class, Map.class }
    )
    @NotNull
    @ToString.Exclude
    @PluginProperty(dynamic = true, group = "source")
    private Object rows;

    @Schema(
        title = "Rows per HTTP request",
        description = """
            Maximum rows in each request. Must be between 1 and 5,000. Defaults to 100. \
            A single execution accepts at most 50,000 rows in total."""
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<@Min(1) @Max(MAX_CHUNK_SIZE) Integer> chunkSize = Property.ofValue(100);

    @Schema(
        title = "Fail when a chunk cannot be sent",
        description = """
            When true, fail after the first chunk exhausts retries. \
            When false, record the failed chunk and continue. Defaults to true."""
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> failOnPartialError = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rWebhookUrl = runContext.render(this.webhookUrl).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("webhookUrl is required: set it to your Clay table webhook URL"));
        var webhookUri = validateWebhookUrl(rWebhookUrl);
        var rAuthToken = runContext.render(this.authToken).as(String.class).orElse(null);
        var rChunkSize = runContext.render(this.chunkSize).as(Integer.class).orElse(100);
        var rFailOnPartialError = runContext.render(this.failOnPartialError).as(Boolean.class).orElse(true);

        if (rChunkSize < 1 || rChunkSize > MAX_CHUNK_SIZE) {
            throw new IllegalArgumentException(
                "chunkSize must be between 1 and " + MAX_CHUNK_SIZE + " but was " + rChunkSize + ": set `chunkSize` to a value in that range"
            );
        }

        List<Map<String, Object>> rRows;
        try {
            rRows = Data.from(this.rows)
                .read(runContext)
                .take(MAX_ROWS_PER_EXECUTION + 1L)
                .collectList()
                .block();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                "Unable to read rows: provide a record, a list of records, or a valid JSON/ION storage URI (" + e.getMessage() + ")", e
            );
        }

        if (rRows == null) {
            throw new IllegalArgumentException("rows could not be resolved: check that the `rows` expression returns a record or a list of records");
        }
        if (rRows.size() > MAX_ROWS_PER_EXECUTION) {
            throw new IllegalArgumentException(
                "Cannot send more than " + MAX_ROWS_PER_EXECUTION + " rows in one task execution: split the input across several executions or webhook sources"
            );
        }
        if (rRows.isEmpty()) {
            return Output.builder().rowCount(0L).chunkCount(0).failedChunks(List.of()).build();
        }

        var chunks = partition(rRows, rChunkSize);
        var failedChunks = new ArrayList<Integer>();
        var successfulRows = 0L;

        var retryer = RetryUtils.Instance.<Void, IllegalStateException>builder()
            .policy(RETRY_POLICY)
            .logger(runContext.logger())
            .failureFunction(PushRows::retriesExhausted)
            .build();

        try (var client = HttpClient.builder()
            .runContext(runContext)
            .configuration(HttpConfiguration.builder()
                .allowFailed(Property.ofValue(true))
                .timeout(TIMEOUT)
                .build())
            .build()) {
            for (var chunkIndex = 0; chunkIndex < chunks.size(); chunkIndex++) {
                var chunk = chunks.get(chunkIndex);
                try {
                    sendChunk(retryer, client, webhookUri, rAuthToken, chunk, chunkIndex);
                    successfulRows += chunk.size();
                } catch (Exception e) {
                    if (e instanceof InterruptedException || e.getCause() instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                    if (rFailOnPartialError) {
                        throw new IllegalStateException(
                            "Clay webhook request failed for chunk " + chunkIndex + ": " + e.getMessage()
                                + ". Check the row payload or set `failOnPartialError: false` to continue on failures.",
                            e
                        );
                    }
                    runContext.logger().warn("Clay webhook request failed for chunk {}: {}; continuing", chunkIndex, e.getMessage());
                    failedChunks.add(chunkIndex);
                }
            }
        }

        runContext.logger().info(
            "Clay push completed: {} of {} rows accepted across {} chunks; {} chunks failed",
            successfulRows, rRows.size(), chunks.size(), failedChunks.size()
        );
        return Output.builder()
            .rowCount(successfulRows)
            .chunkCount(chunks.size())
            .failedChunks(failedChunks)
            .build();
    }

    private static URI validateWebhookUrl(String value) {
        final var message = "webhookUrl must be a valid absolute HTTP(S) URL: set it to the webhook URL shown in your Clay table";
        URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(message, e);
        }
        if (!uri.isAbsolute() || uri.getHost() == null || !("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))) {
            throw new IllegalArgumentException(message);
        }
        return uri;
    }

    private static List<List<Map<String, Object>>> partition(List<Map<String, Object>> rows, int chunkSize) {
        var chunks = new ArrayList<List<Map<String, Object>>>((rows.size() + chunkSize - 1) / chunkSize);
        for (var start = 0; start < rows.size(); start += chunkSize) {
            chunks.add(rows.subList(start, Math.min(start + chunkSize, rows.size())));
        }
        return chunks;
    }

    private static IllegalStateException retriesExhausted(RetryUtils.RetryFailed failed) {
        var cause = failed.getCause();
        var message = cause instanceof RetryableClayException retryable
            ? retryable.summary + " after " + failed.getAttemptCount() + " attempts" + retryable.detail
            : cause.getMessage() + " after " + failed.getAttemptCount() + " attempts";
        return new IllegalStateException(message, cause);
    }

    private static void sendChunk(
        RetryUtils.Instance<Void, IllegalStateException> retryer,
        HttpClient client,
        URI webhookUri,
        String authToken,
        List<Map<String, Object>> rows,
        int chunkIndex
    ) {
        retryer.runRetryIf(
            throwable -> throwable instanceof RetryableClayException,
            () ->
            {
                var requestBuilder = HttpRequest.builder()
                    .method("POST")
                    .uri(webhookUri)
                    .body(HttpRequest.JsonRequestBody.builder().content(rows).build());

                if (authToken != null && !authToken.isBlank()) {
                    requestBuilder.addHeader("Authorization", "Bearer " + authToken);
                }

                HttpResponse<String> response;
                try {
                    response = client.request(requestBuilder.build(), String.class);
                } catch (HttpClientException e) {
                    throw new RetryableClayException("Clay webhook is unreachable for chunk " + chunkIndex + " (" + e.getMessage() + ")", "", e);
                }

                var statusCode = response.getStatus().getCode();
                if (statusCode >= 200 && statusCode < 300) {
                    return null;
                }

                var detail = bodyExcerpt(response.getBody());
                var summary = "Clay returned HTTP " + statusCode + " for chunk " + chunkIndex;
                if (statusCode == 429 || statusCode >= 500) {
                    if (statusCode == 429) {
                        waitForRetryAfter(response);
                    }
                    throw new RetryableClayException(summary, detail, null);
                }
                throw new IllegalStateException(summary + detail);
            }
        );
    }

    private static void waitForRetryAfter(HttpResponse<String> response) throws InterruptedException {
        var retryAfter = response.getHeaders().firstValue("Retry-After").orElse(null);
        if (retryAfter == null) {
            return;
        }
        try {
            var seconds = Math.min(Long.parseLong(retryAfter.trim()), MAX_RETRY_AFTER_SECONDS);
            if (seconds > 0) {
                Thread.sleep(Duration.ofSeconds(seconds));
            }
        } catch (NumberFormatException ignored) {
            // HTTP-date form is not supported; fall back to the exponential policy
        }
    }

    private static String bodyExcerpt(String body) {
        if (body == null || body.isBlank()) {
            return "";
        }
        var trimmed = body.strip();
        return ": " + (trimmed.length() > ERROR_BODY_EXCERPT_LENGTH ? trimmed.substring(0, ERROR_BODY_EXCERPT_LENGTH) + "..." : trimmed);
    }

    private static class RetryableClayException extends RuntimeException {
        private final String summary;
        private final String detail;

        private RetryableClayException(String summary, String detail, Throwable cause) {
            super(summary + detail, cause);
            this.summary = summary;
            this.detail = detail;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of rows accepted by Clay",
            description = """
                Rows contained in the chunks Clay accepted. Rows from failed chunks are excluded."""
        )
        private final Long rowCount;

        @Schema(
            title = "Number of logical chunks submitted",
            description = """
                Logical chunks the input was split into using `chunkSize`. \
                This is not the number of raw HTTP attempts: retries are not counted."""
        )
        private final Integer chunkCount;

        @Schema(
            title = "Zero-based indices of failed chunks",
            description = """
                Chunks that still failed after retries. Only populated when `failOnPartialError` is false."""
        )
        private final List<Integer> failedChunks;
    }
}
