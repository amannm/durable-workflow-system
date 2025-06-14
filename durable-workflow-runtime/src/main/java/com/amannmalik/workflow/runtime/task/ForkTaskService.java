package com.amannmalik.workflow.runtime.task;

import com.amannmalik.workflow.runtime.Services;
import dev.restate.sdk.Context;
import dev.restate.sdk.DurableFuture;
import dev.restate.sdk.InvocationHandle;
import io.serverlessworkflow.api.types.ForkTask;
import io.serverlessworkflow.api.types.TaskItem;

import java.time.Duration;
import java.util.stream.Collectors;

@dev.restate.sdk.annotation.Service
public class ForkTaskService {

    @dev.restate.sdk.annotation.Handler
    public void execute(Context ctx, ForkTask task) {
        var fork = task.getFork();
        DurableFuture.all(fork.getBranches().stream()
                .map(TaskItem::getTask)
                .map(subTask -> Services.invokeService(ctx, "WorkflowTaskService", "execute", subTask, Void.class, Duration.ZERO))
                .map(InvocationHandle::attach)
                .collect(Collectors.toUnmodifiableList())).await();
    }
}
