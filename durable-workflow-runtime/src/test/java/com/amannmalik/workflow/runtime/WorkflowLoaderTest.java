package com.amannmalik.workflow.runtime;

import io.serverlessworkflow.api.types.Workflow;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

class WorkflowLoaderTest {

    @Test
    void loadSampleWorkflow() {
        InputStream in = getClass().getResourceAsStream("/sample.yaml");
        assertNotNull(in);
        Workflow wf = WorkflowLoader.fromYaml(in);
        assertEquals("switch-example", wf.getDocument().getName());
        assertFalse(wf.getDo().isEmpty());
    }
}
