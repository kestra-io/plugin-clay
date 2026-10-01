package io.kestra.plugin.clay;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.serializers.YamlParser;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@KestraTest
class PushRowsFlowValidationTest {
    @Inject
    private ModelValidator modelValidator;

    @Test
    void sanityCheckFlowsAreValid() throws Exception {
        var url = Objects.requireNonNull(getClass().getClassLoader().getResource("sanity-checks"));
        try (var paths = Files.list(Path.of(url.toURI()))) {
            var flows = paths.filter(p -> p.toString().endsWith(".yaml")).map(Path::toFile).toList();
            assertThat(flows.size(), greaterThan(0));
            for (File file : flows) {
                assertDoesNotThrow(() -> modelValidator.validate(YamlParser.parse(file, Flow.class)), file.getName());
            }
        }
    }

    @Test
    void flowWithChunkSizeConstraintsIsValid() {
        var yaml = """
            id: clay_chunk_size
            namespace: company.team

            tasks:
              - id: push
                type: io.kestra.plugin.clay.PushRows
                webhookUrl: "{{ secret('CLAY_WEBHOOK_URL') }}"
                rows:
                  - email: alice@example.com
                chunkSize: 250
            """;

        var flow = YamlParser.parse(yaml, Flow.class);

        assertDoesNotThrow(() -> modelValidator.validate(flow));
    }
}
