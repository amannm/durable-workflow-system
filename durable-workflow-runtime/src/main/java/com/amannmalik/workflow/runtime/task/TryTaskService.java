package com.amannmalik.workflow.runtime.task;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import com.amannmalik.workflow.runtime.DefinitionHelper;
import io.serverlessworkflow.api.types.TaskItem;
import io.serverlessworkflow.api.types.TryTask;
import io.serverlessworkflow.api.types.TryTaskCatch;

import java.util.List;

public class TryTaskService {

    public static final ServiceDefinition DEFINITION = DefinitionHelper.taskService(
            TryTaskService.class,
            TryTask.class,
            TryTaskService::execute
    );

    public static void execute(WorkflowContext ctx, TryTask task) {
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
