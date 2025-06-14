package com.amannmalik.workflow.runtime.task;

import com.amannmalik.workflow.runtime.Services;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.sdk.WorkflowContext;
import io.serverlessworkflow.api.types.CallTask;
import io.serverlessworkflow.api.types.DoTask;
import io.serverlessworkflow.api.types.EmitTask;
import io.serverlessworkflow.api.types.ForTask;
import io.serverlessworkflow.api.types.ForkTask;
import io.serverlessworkflow.api.types.ListenTask;
import io.serverlessworkflow.api.types.RaiseTask;
import io.serverlessworkflow.api.types.RunTask;
import io.serverlessworkflow.api.types.SetTask;
import io.serverlessworkflow.api.types.SwitchTask;
import io.serverlessworkflow.api.types.Task;
import io.serverlessworkflow.api.types.TryTask;
import io.serverlessworkflow.api.types.WaitTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@dev.restate.sdk.annotation.Service
public class WorkflowTaskService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowTaskService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @dev.restate.sdk.annotation.Handler
    public void execute(WorkflowContext ctx, Task task) {
        switch (task.get()) {
            case CallTask x -> Services.callService(ctx, "CallTaskService", "execute", x, Void.class);
            case DoTask x ->
                    x.getDo().forEach(t -> Services.callService(ctx, "WorkflowTaskService", "execute", t.getTask(), Void.class));
            case ForkTask x -> Services.callService(ctx, "ForkTaskervice", "execute", x, Void.class);
            case EmitTask x -> Services.callService(ctx, "EmitTaskService", "execute", x, Void.class);
            case ForTask x -> x.getDo().forEach(t -> execute(ctx, t.getTask()));
            case ListenTask x -> log.info("Listen task not implemented: {}", x);
            case RaiseTask x -> logRaise(x);
            // TODO: these as well
            case RunTask x -> runTaskService.execute(ctx, x);
            case SetTask x -> setTaskService.execute(ctx, x);
            case SwitchTask x -> switchTaskService.execute(ctx, x);
            case TryTask x -> tryTaskService.execute(ctx, x);
            case WaitTask x -> waitTaskService.execute(ctx, x);
            default -> throw new UnsupportedOperationException("Unexpected task: " + task.get());
        }
    }

    private void logRaise(RaiseTask x) {
        try {
            log.warn("Raise event: {}", MAPPER.writeValueAsString(x.getRaise()));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
