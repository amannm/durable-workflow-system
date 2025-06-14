package com.amannmalik.workflow.runtime.task;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.DurableFuture;
import dev.restate.sdk.common.RetryPolicy;
import dev.restate.serde.TypeTag;
import io.serverlessworkflow.api.types.ForkTask;
import io.serverlessworkflow.api.types.TaskItem;

@dev.restate.sdk.annotation.Service
public class ForkTaskService {

    @dev.restate.sdk.annotation.Handler
    public void execute(WorkflowContext ctx, ForkTask task) {
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
