package com.amannmalik.workflow.runtime.task.call.handler;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.CallAsyncAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncApiCallHandler implements CallHandler<CallAsyncAPI> {
  private static final Logger log = LoggerFactory.getLogger(AsyncApiCallHandler.class);
  private final StateKey<Object> resultKey;

  public AsyncApiCallHandler(StateKey<Object> resultKey) {
    this.resultKey = resultKey;
  }

  @Override
  public void handle(WorkflowContext ctx, CallAsyncAPI call) {
    log.info("Call AsyncAPI not implemented: {}", call);
  }
}
