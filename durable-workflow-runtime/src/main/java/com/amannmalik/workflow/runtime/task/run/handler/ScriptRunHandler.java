package com.amannmalik.workflow.runtime.task.run.handler;

import dev.restate.sdk.WorkflowContext;
import io.serverlessworkflow.api.types.RunScript;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScriptRunHandler implements RunHandler<RunScript> {
  private static final Logger log = LoggerFactory.getLogger(ScriptRunHandler.class);

  @Override
  public void handle(WorkflowContext ctx, RunScript run) {
    log.info("Run script not implemented: {}", run);
  }
}
