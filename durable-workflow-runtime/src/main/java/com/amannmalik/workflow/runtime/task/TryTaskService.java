package com.amannmalik.workflow.runtime.task;

import dev.restate.sdk.HandlerRunner;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.endpoint.definition.HandlerDefinition;
import dev.restate.sdk.endpoint.definition.HandlerType;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import dev.restate.sdk.endpoint.definition.ServiceType;
import dev.restate.serde.Serde;
import dev.restate.serde.jackson.JacksonSerdeFactory;
import dev.restate.serde.jackson.JacksonSerdes;
import io.serverlessworkflow.api.types.TaskItem;
import io.serverlessworkflow.api.types.TryTask;
import io.serverlessworkflow.api.types.TryTaskCatch;

import java.util.List;

public class TryTaskService {

    public static final ServiceDefinition DEFINITION = ServiceDefinition.of(
            "TryTaskService",
            ServiceType.SERVICE,
            List.of(
                    HandlerDefinition.of(
                            "execute",
                            HandlerType.SHARED,
                            JacksonSerdes.of(TryTask.class),
                            Serde.VOID,
                            HandlerRunner.of(TryTaskService::execute, JacksonSerdeFactory.DEFAULT, HandlerRunner.Options.DEFAULT)
                    )
            )
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
