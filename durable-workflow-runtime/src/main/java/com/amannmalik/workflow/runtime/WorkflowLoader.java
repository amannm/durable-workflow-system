package com.amannmalik.workflow.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.serverlessworkflow.api.types.Workflow;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/**
 * Utility class to load {@link Workflow} definitions from YAML documents.
 */
public final class WorkflowLoader {

    private WorkflowLoader() {
    }

    public static Workflow fromYaml(InputStream in) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.registerModule(new JavaTimeModule());
        try {
            return mapper.readValue(in, Workflow.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
