package com.amannmalik.workflow.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.serverlessworkflow.api.types.Workflow;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

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
