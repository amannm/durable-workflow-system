package com.amannmalik.workflow.runtime.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.sdk.WorkflowContext;
import io.serverlessworkflow.api.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@dev.restate.sdk.annotation.Service
public class WorkflowTaskService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowTaskService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CallTaskService callTaskService = new CallTaskService();
    private final ForkTaskService forkTaskService = new ForkTaskService();
    private final EmitTaskService emitTaskService = new EmitTaskService();
    private final RunTaskService runTaskService = new RunTaskService();
    private final SetTaskService setTaskService = new SetTaskService();
    private final SwitchTaskService switchTaskService = new SwitchTaskService();
    private final TryTaskService tryTaskService = new TryTaskService();
    private final WaitTaskService waitTaskService = new WaitTaskService();

    @dev.restate.sdk.annotation.Handler
    public void execute(WorkflowContext ctx, Task task) {
        switch (task.get()) {
            case CallTask x -> callTaskService.execute(ctx, x);
            case DoTask x -> x.getDo().forEach(t -> execute(ctx, t.getTask()));
            case ForkTask x -> forkTaskService.execute(ctx, x);
            case EmitTask x -> emitTaskService.execute(ctx, x);
            case ForTask x -> x.getDo().forEach(t -> execute(ctx, t.getTask()));
            case ListenTask x -> log.info("Listen task not implemented: {}", x);
            case RaiseTask x -> logRaise(x);
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
