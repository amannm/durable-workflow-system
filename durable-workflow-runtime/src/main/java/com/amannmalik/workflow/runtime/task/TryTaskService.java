package com.amannmalik.workflow.runtime.task;

import dev.restate.sdk.WorkflowContext;
import io.serverlessworkflow.api.types.TaskItem;
import io.serverlessworkflow.api.types.TryTask;
import io.serverlessworkflow.api.types.TryTaskCatch;

@dev.restate.sdk.annotation.Service
public class TryTaskService {

    @dev.restate.sdk.annotation.Handler
    public void execute(WorkflowContext ctx, TryTask task) {
        java.util.List<TaskItem> aTry = task.getTry();
        TryTaskCatch aCatch = task.getCatch();
        try {
            for (var ti : aTry) {
                new WorkflowTaskService().execute(ctx, ti.getTask());
            }
        } catch (Exception e) {
            if (aCatch != null) {
                for (var ti : aCatch.getDo()) {
                    new WorkflowTaskService().execute(ctx, ti.getTask());
                }
            } else {
                throw e;
            }
        }
    }
}
