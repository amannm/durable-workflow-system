package com.amannmalik.workflow.runtime.task;

import com.amannmalik.workflow.runtime.DefinitionHelper;
import com.amannmalik.workflow.runtime.Services;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.sdk.Context;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import dev.restate.sdk.endpoint.definition.ServiceType;
import dev.restate.sdk.endpoint.definition.HandlerType;
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

import java.lang.invoke.MethodHandles;
import java.util.List;

public class WorkflowTaskService {

    public static final ServiceDefinition DEFINITION = DefinitionHelper.singleVoidHandlerService(
            MethodHandles.lookup().lookupClass().getCanonicalName(),
            ServiceType.SERVICE,
            "execute",
            HandlerType.SHARED,
            Task.class,
            WorkflowTaskService::execute
    );

    private static final Logger log = LoggerFactory.getLogger(WorkflowTaskService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void execute(Context ctx, Task task) {
        switch (task.get()) {
            case CallTask x -> await(Services.callService(ctx, "CallTaskService", "execute", x, Void.class));
            case DoTask x ->
                    x.getDo().forEach(t -> await(Services.callService(ctx, "WorkflowTaskService", "execute", t.getTask(), Void.class)));
            case ForkTask x -> await(Services.callService(ctx, "ForkTaskService", "execute", x, Void.class));
            case EmitTask x -> await(Services.callService(ctx, "EmitTaskService", "execute", x, Void.class));
            case ForTask x -> x.getDo().forEach(t -> execute(ctx, t.getTask()));
            case ListenTask x -> await(Services.callService(ctx, "ListenTaskService", "execute", x, Void.class));
            case RaiseTask x -> await(Services.callService(ctx, "RaiseTaskService", "execute", x, Void.class));
            case RunTask x -> await(Services.callService(ctx, "RunTaskService", "execute", x, Void.class));
            case SetTask x -> await(Services.callService(ctx, "SetTaskService", "execute", x, Void.class));
            case SwitchTask x -> await(Services.callService(ctx, "SwitchTaskService", "execute", x, Void.class));
            case TryTask x -> await(Services.callService(ctx, "TryTaskService", "execute", x, Void.class));
            case WaitTask x -> await(Services.callService(ctx, "WaitTaskService", "execute", x, Void.class));
            default -> throw new UnsupportedOperationException("Unexpected task: " + task.get());
        }
    }

    private static void await(dev.restate.sdk.CallDurableFuture<?> future) {
        if (future != null) {
            future.await();
        }
    }
}
