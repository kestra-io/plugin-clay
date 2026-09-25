package io.kestra.plugin.clay;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class PushRowsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storage;

    private WireMockServer wireMockServer;

    @BeforeEach
    void setUp() {
        wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        wireMockServer.start();
    }

    @AfterEach
    void tearDown() {
        wireMockServer.stop();
    }

    @Test
    void sendsRowsInOrderedChunksWithBearerAuthentication() throws Exception {
        wireMockServer.stubFor(post(urlEqualTo("/clay-hook"))
            .willReturn(aResponse().withStatus(204)));

        var task = task(List.of(
            Map.of("email", "one@example.com"),
            Map.of("email", "two@example.com"),
            Map.of("email", "three@example.com")
        )).authToken(Property.ofValue("test-token"))
            .chunkSize(Property.ofValue(2))
            .build();

        var output = task.run(runContextFactory.of(Map.of()));

        assertThat(output.getRowCount(), is(3L));
        assertThat(output.getChunkCount(), is(2));
        assertThat(output.getFailedChunks(), is(empty()));
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/clay-hook"))
            .withHeader("Content-Type", containing("application/json"))
            .withHeader("Authorization", equalTo("Bearer test-token"))
            .withRequestBody(equalToJson("[{\"email\":\"one@example.com\"},{\"email\":\"two@example.com\"}]")));
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/clay-hook"))
            .withRequestBody(equalToJson("[{\"email\":\"three@example.com\"}]")));
    }

    @Test
    void readsRowsFromKestraStorageUri() throws Exception {
        var records = List.of(
            Map.of("email", "one@example.com", "position", 1),
            Map.of("email", "two@example.com", "position", 2)
        );
        var serializedRows = new StringWriter();
        FileSerde.writeAll(serializedRows, Flux.fromIterable(records)).block();

        URI rowsUri;
        try (var input = new ByteArrayInputStream(serializedRows.toString().getBytes(StandardCharsets.UTF_8))) {
            rowsUri = storage.put(TenantService.MAIN_TENANT, null, URI.create("/clay-rows.ion"), input);
        }
        wireMockServer.stubFor(post(urlEqualTo("/clay-hook"))
            .willReturn(aResponse().withStatus(204)));

        var output = PushRows.builder()
            .webhookUrl(Property.ofValue(wireMockServer.baseUrl() + "/clay-hook"))
            .rows(rowsUri.toString())
            .chunkSize(Property.ofValue(1))
            .build()
            .run(runContextFactory.of(Map.of()));

        assertThat(output.getRowCount(), is(2L));
        assertThat(output.getChunkCount(), is(2));
        assertThat(output.getFailedChunks(), is(empty()));

        var requests = new ArrayList<>(wireMockServer.getAllServeEvents());
        Collections.reverse(requests);
        assertThat(requests, hasSize(2));
        assertThat(
            JacksonMapper.ofJson().readTree(requests.get(0).getRequest().getBodyAsString()).get(0).get("position").asInt(),
            is(1)
        );
        assertThat(
            JacksonMapper.ofJson().readTree(requests.get(1).getRequest().getBodyAsString()).get(0).get("position").asInt(),
            is(2)
        );
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/clay-hook"))
            .withRequestBody(equalToJson("[{\"email\":\"one@example.com\",\"position\":1}]")));
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/clay-hook"))
            .withRequestBody(equalToJson("[{\"email\":\"two@example.com\",\"position\":2}]")));
    }

    @Test
    void retriesRateLimitAndCountsLogicalChunks() throws Exception {
        wireMockServer.stubFor(post(urlEqualTo("/clay-hook"))
            .inScenario("rate-limit-retry")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse().withStatus(429))
            .willSetStateTo("retry-ready"));
        wireMockServer.stubFor(post(urlEqualTo("/clay-hook"))
            .inScenario("rate-limit-retry")
            .whenScenarioStateIs("retry-ready")
            .willReturn(aResponse().withStatus(200)));

        var output = task(List.of(Map.of("email", "one@example.com")))
            .build()
            .run(runContextFactory.of(Map.of()));

        assertThat(output.getRowCount(), is(1L));
        assertThat(output.getChunkCount(), is(1));
        wireMockServer.verify(2, postRequestedFor(urlEqualTo("/clay-hook")));
    }

    @Test
    void retriesServerErrors() throws Exception {
        wireMockServer.stubFor(post(urlEqualTo("/clay-hook"))
            .inScenario("server-error-retry")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse().withStatus(503))
            .willSetStateTo("retry-ready"));
        wireMockServer.stubFor(post(urlEqualTo("/clay-hook"))
            .inScenario("server-error-retry")
            .whenScenarioStateIs("retry-ready")
            .willReturn(aResponse().withStatus(200)));

        var output = task(List.of(Map.of("email", "one@example.com")))
            .build()
            .run(runContextFactory.of(Map.of()));

        assertThat(output.getRowCount(), is(1L));
        assertThat(output.getChunkCount(), is(1));
        wireMockServer.verify(2, postRequestedFor(urlEqualTo("/clay-hook")));
    }

    @Test
    void continuesAfterFailedChunkWhenBestEffortIsEnabled() throws Exception {
        wireMockServer.stubFor(post(urlEqualTo("/clay-hook"))
            .inScenario("partial-failure")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(aResponse().withStatus(400))
            .willSetStateTo("first-chunk-failed"));
        wireMockServer.stubFor(post(urlEqualTo("/clay-hook"))
            .inScenario("partial-failure")
            .whenScenarioStateIs("first-chunk-failed")
            .willReturn(aResponse().withStatus(200)));

        var output = task(List.of(
            Map.of("email", "one@example.com"),
            Map.of("email", "two@example.com")
        ))
            .chunkSize(Property.ofValue(1))
            .failOnPartialError(Property.ofValue(false))
            .build()
            .run(runContextFactory.of(Map.of()));

        assertThat(output.getRowCount(), is(1L));
        assertThat(output.getChunkCount(), is(2));
        assertThat(output.getFailedChunks(), contains(0));
        wireMockServer.verify(2, postRequestedFor(urlEqualTo("/clay-hook")));
    }

    @Test
    void failsFastByDefaultOnNonRetryableResponse() throws Exception {
        wireMockServer.stubFor(post(urlEqualTo("/clay-hook"))
            .willReturn(aResponse().withStatus(400)));

        var task = task(List.of(
            Map.of("email", "one@example.com"),
            Map.of("email", "two@example.com")
        )).chunkSize(Property.ofValue(1)).build();

        assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of(Map.of())));
        wireMockServer.verify(1, postRequestedFor(urlEqualTo("/clay-hook")));
    }

    @Test
    void emptyRowsReturnZeroCountsWithoutRequest() throws Exception {
        var output = task(List.of()).build().run(runContextFactory.of(Map.of()));

        assertThat(output.getRowCount(), is(0L));
        assertThat(output.getChunkCount(), is(0));
        assertThat(output.getFailedChunks(), is(empty()));
        wireMockServer.verify(0, anyRequestedFor(anyUrl()));
    }

    @Test
    void rejectsMoreThanClaySubmissionLimitBeforeSending() throws Exception {
        var rows = java.util.Collections.nCopies(50_001, Map.<String, Object>of("email", "one@example.com"));
        var task = task(rows).build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of(Map.of())));
        wireMockServer.verify(0, anyRequestedFor(anyUrl()));
    }

    @Test
    void rejectsNonHttpWebhookUrl() throws Exception {
        var task = PushRows.builder()
            .webhookUrl(Property.ofValue("ftp://example.com/hook"))
            .rows(List.of(Map.of("email", "one@example.com")))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of(Map.of())));
        wireMockServer.verify(0, anyRequestedFor(anyUrl()));
    }

    private PushRows.PushRowsBuilder<?, ?> task(List<Map<String, Object>> rows) {
        return PushRows.builder()
            .webhookUrl(Property.ofValue(wireMockServer.baseUrl() + "/clay-hook"))
            .rows(rows);
    }
}
