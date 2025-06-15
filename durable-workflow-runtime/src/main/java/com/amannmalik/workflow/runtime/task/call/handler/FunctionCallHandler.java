package com.amannmalik.workflow.runtime.task.call.handler;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.CallFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionCallHandler implements CallHandler<CallFunction> {
  private static final Logger log = LoggerFactory.getLogger(FunctionCallHandler.class);
  private final StateKey<Object> resultKey;

  public FunctionCallHandler(StateKey<Object> resultKey) {
    this.resultKey = resultKey;
  }

  @Override
  public void handle(WorkflowContext ctx, CallFunction call) {
    log.info("Call function not implemented: {}", call);
  }
}
