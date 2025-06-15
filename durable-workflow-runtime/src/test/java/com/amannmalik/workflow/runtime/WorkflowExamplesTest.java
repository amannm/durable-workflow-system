package com.amannmalik.workflow.runtime;

import io.serverlessworkflow.api.types.Workflow;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class WorkflowExamplesTest {

    static Stream<String> yamlFiles() throws IOException {
        return Files.list(Path.of("src/test/resources"))
                .filter(p -> p.getFileName().toString().endsWith(".yaml"))
                .map(p -> p.getFileName().toString());
    }

    @ParameterizedTest
    @MethodSource("yamlFiles")
    void loadWorkflowExample(String fileName) {
        InputStream in = getClass().getResourceAsStream("/" + fileName);
        assertNotNull(in, "resource " + fileName + " not found");
        Workflow wf = WorkflowLoader.fromYaml(in);
        assertNotNull(wf.getDocument());
        assertFalse(wf.getDo().isEmpty());
    }
}
