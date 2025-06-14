package com.amannmalik.workflow.runtime.task;

import dev.restate.sdk.HandlerRunner;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.DurableFuture;
import dev.restate.sdk.common.RetryPolicy;
import dev.restate.sdk.endpoint.definition.HandlerDefinition;
import dev.restate.sdk.endpoint.definition.HandlerType;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import dev.restate.sdk.endpoint.definition.ServiceType;
import dev.restate.serde.Serde;
import dev.restate.serde.TypeTag;
import dev.restate.serde.jackson.JacksonSerdeFactory;
import dev.restate.serde.jackson.JacksonSerdes;
import io.serverlessworkflow.api.types.ForkTask;
import io.serverlessworkflow.api.types.TaskItem;

import java.util.List;

public class ForkTaskService {

    public static final ServiceDefinition DEFINITION = ServiceDefinition.of(
            "ForkTaskService",
            ServiceType.SERVICE,
            List.of(
                    HandlerDefinition.of(
                            "execute",
                            HandlerType.SHARED,
                            JacksonSerdes.of(ForkTask.class),
                            Serde.VOID,
                            HandlerRunner.of(ForkTaskService::execute, JacksonSerdeFactory.DEFAULT, HandlerRunner.Options.DEFAULT)
                    )
            )
    );

    public static void execute(WorkflowContext ctx, ForkTask task) {
        var fork = task.getFork();
        int i = 0;
        java.util.List<DurableFuture<Void>> futures = new java.util.ArrayList<>();
        for (TaskItem item : fork.getBranches()) {
            String name = "branch-" + (i++);
            futures.add(ctx.runAsync(name, TypeTag.of(Void.class), RetryPolicy.defaultPolicy(), () -> {
                new WorkflowTaskService().execute(ctx, item.getTask());
                return null;
            }));
        }
        for (DurableFuture<Void> f : futures) {
            f.await();
        }
    }
}
