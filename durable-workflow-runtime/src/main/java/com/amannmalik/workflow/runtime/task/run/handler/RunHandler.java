package com.amannmalik.workflow.runtime.task.run.handler;

import dev.restate.sdk.WorkflowContext;

public sealed interface RunHandler<T>
        permits ContainerRunHandler, WorkflowRunHandler, ScriptRunHandler, ShellRunHandler {
    void handle(WorkflowContext ctx, T run);
}
