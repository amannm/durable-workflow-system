package com.amannmalik.workflow.runtime.task;

import dev.restate.sdk.WorkflowContext;
import io.serverlessworkflow.api.types.Task;

/**
 * Executes individual workflow tasks by delegating to task specific services.
 */
public final class TaskExecutor {

    private TaskExecutor() {
    }

    public static void execute(WorkflowContext ctx, Task task) {
        new WorkflowTaskService().execute(ctx, task);
    }
}
