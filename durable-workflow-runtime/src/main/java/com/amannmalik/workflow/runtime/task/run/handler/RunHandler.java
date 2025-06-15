package com.amannmalik.workflow.runtime.task.run.handler;

import dev.restate.sdk.WorkflowContext;

public interface RunHandler<T> {
    void handle(WorkflowContext ctx, T run);
}
