package com.amannmalik.workflow.runtime.task.call.handler;

import dev.restate.sdk.WorkflowContext;

public interface CallHandler<T> {
  void handle(WorkflowContext ctx, T call);
}
