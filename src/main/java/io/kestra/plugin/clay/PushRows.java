package io.kestra.plugin.clay;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.configurations.HttpConfiguration;
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
    description = "Sends records to a Clay table through its inbound webhook, in sequential batches."
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
    private static final Exponential RETRY_POLICY = Exponential.builder()
        .interval(Duration.ofSeconds(1))
        .maxInterval(Duration.ofSeconds(10))
        .delayFactor(2.0)
        .maxAttempts(3)
        .build();

    @Schema(
        title = "Clay webhook URL",
        description = "Absolute HTTP(S) URL generated for the Clay table webhook. Store it as a Kestra secret."
    )
    @NotNull
    @ToString.Exclude
    @PluginProperty(group = "connection", secret = true)
    private Property<String> webhookUrl;

    @Schema(
        title = "Clay webhook authentication token",
        description = "Optional token sent as a Bearer authorization header. Store it as a Kestra secret."
    )
    @ToString.Exclude
    @PluginProperty(group = "connection", secret = true)
    private Property<String> authToken;

    @Schema(
        title = "Rows to send",
        description = "A record, list of records, JSON string, or Kestra storage URI containing records.",
        anyOf = { String.class, List.class, Map.class }
    )
    @NotNull
    @ToString.Exclude
    @PluginProperty(group = "source")
    private Object rows;

    @Schema(
        title = "Rows per HTTP request",
        description = "Maximum rows in each request. Must be between 1 and 50,000. Defaults to 100."
    )
    @Builder.Default
    @Min(1)
    @Max(MAX_ROWS_PER_EXECUTION)
    @PluginProperty(group = "processing")
    private Property<Integer> chunkSize = Property.ofValue(100);

    @Schema(
        title = "Fail when a chunk cannot be sent",
        description = "When true, fail after the first chunk exhausts retries. When false, record the failed chunk and continue. Defaults to true."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> failOnPartialError = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rWebhookUrl = runContext.render(this.webhookUrl).as(String.class).orElseThrow();
        URI webhookUri = validateWebhookUrl(rWebhookUrl);
        var rAuthToken = runContext.render(this.authToken).as(String.class).orElse(null);
        var rChunkSize = runContext.render(this.chunkSize).as(Integer.class).orElse(100);
        var rFailOnPartialError = runContext.render(this.failOnPartialError).as(Boolean.class).orElse(true);

        if (rChunkSize < 1 || rChunkSize > MAX_ROWS_PER_EXECUTION) {
            throw new IllegalArgumentException("chunkSize must be between 1 and " + MAX_ROWS_PER_EXECUTION);
        }

        List<Map<String, Object>> rRows;
        try {
            rRows = Data.from(this.rows)
                .read(runContext)
                .take(MAX_ROWS_PER_EXECUTION + 1L)
                .collectList()
                .block();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Unable to read rows; provide a record, a list of records, or a valid JSON/ION storage URI", e);
        }

        if (rRows == null) {
            throw new IllegalArgumentException("rows could not be resolved");
        }
        if (rRows.size() > MAX_ROWS_PER_EXECUTION) {
            throw new IllegalArgumentException("Cannot send more than " + MAX_ROWS_PER_EXECUTION + " rows in one task execution");
        }
        if (rRows.isEmpty()) {
            return Output.builder().rowCount(0L).chunkCount(0).failedChunks(List.of()).build();
        }

        List<List<Map<String, Object>>> chunks = partition(rRows, rChunkSize);
        List<Integer> failedChunks = new ArrayList<>();
        long successfulRows = 0;

        try (HttpClient client = HttpClient.builder()
            .runContext(runContext)
            .configuration(HttpConfiguration.builder().allowFailed(Property.ofValue(true)).build())
            .build()) {
            for (int chunkIndex = 0; chunkIndex < chunks.size(); chunkIndex++) {
                List<Map<String, Object>> chunk = chunks.get(chunkIndex);
                try {
                    sendChunkWithRetry(client, webhookUri, rAuthToken, chunk, chunkIndex, runContext);
                    successfulRows += chunk.size();
                } catch (RuntimeException e) {
                    if (rFailOnPartialError) {
                        throw new IllegalStateException(
                            "Clay webhook request failed for chunk " + chunkIndex + " (" + e.getClass().getSimpleName() + ")"
                        );
                    }
                    runContext.logger().warn("Clay webhook request failed for chunk {} after retries; continuing", chunkIndex);
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
        try {
            URI uri = URI.create(value);
            if (!uri.isAbsolute() || uri.getHost() == null || !("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))) {
                throw new IllegalArgumentException("webhookUrl must be an absolute HTTP(S) URL");
            }
            return uri;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("webhookUrl must be a valid absolute HTTP(S) URL");
        }
    }

    private static List<List<Map<String, Object>>> partition(List<Map<String, Object>> rows, int chunkSize) {
        List<List<Map<String, Object>>> chunks = new ArrayList<>((rows.size() + chunkSize - 1) / chunkSize);
        for (int start = 0; start < rows.size(); start += chunkSize) {
            chunks.add(rows.subList(start, Math.min(start + chunkSize, rows.size())));
        }
        return chunks;
    }

    private static void sendChunkWithRetry(
        HttpClient client,
        URI webhookUri,
        String authToken,
        List<Map<String, Object>> rows,
        int chunkIndex,
        RunContext runContext
    ) {
        RetryUtils.Instance.<Void, RuntimeException>builder()
            .policy(RETRY_POLICY)
            .logger(runContext.logger())
            .failureFunction(failed -> new IllegalStateException(
                "Clay request failed after " + failed.getAttemptCount() + " attempts"
            ))
            .build()
            .runRetryIf(
                throwable -> throwable instanceof RetryableHttpStatusException,
                () ->
                {
                    HttpRequest.HttpRequestBuilder requestBuilder = HttpRequest.builder()
                        .method("POST")
                        .uri(webhookUri)
                        .addHeader("Content-Type", "application/json")
                        .body(HttpRequest.JsonRequestBody.builder().content(rows).build());

                    if (authToken != null && !authToken.isBlank()) {
                        requestBuilder.addHeader("Authorization", "Bearer " + authToken);
                    }

                    HttpResponse<String> response = client.request(requestBuilder.build(), String.class);
                    int statusCode = response.getStatus().getCode();
                    if (statusCode < 200 || statusCode >= 300) {
                        if (statusCode == 429 || statusCode >= 500) {
                            throw new RetryableHttpStatusException("HTTP " + statusCode + " for chunk " + chunkIndex);
                        }
                        throw new IllegalStateException("Clay returned HTTP " + statusCode + " for chunk " + chunkIndex);
                    }
                    return null;
                }
            );
    }

    private static class RetryableHttpStatusException extends RuntimeException {
        private RetryableHttpStatusException(String message) {
            super(message);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of rows accepted by Clay")
        private final Long rowCount;

        @Schema(title = "Number of logical chunks processed")
        private final Integer chunkCount;

        @Schema(title = "Zero-based indices of chunks that failed after retries")
        private final List<Integer> failedChunks;
    }
}
