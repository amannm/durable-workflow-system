package com.amannmalik.workflow.runtime.task;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.SetTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@dev.restate.sdk.annotation.Service
public class SetTaskService {

    private static final Logger log = LoggerFactory.getLogger(SetTaskService.class);

    @dev.restate.sdk.annotation.Handler
    public void execute(WorkflowContext ctx, SetTask task) {
        var s = task.getSet();
        var k = s.getString();
        var v = s.get();
        switch (v) {
            case String sv -> ctx.set(StateKey.of(k, String.class), sv);
            case Long sl -> ctx.set(StateKey.of(k, Long.class), sl);
            case Integer si -> ctx.set(StateKey.of(k, Integer.class), si);
            case Double sd -> ctx.set(StateKey.of(k, Double.class), sd);
            case Boolean sb -> ctx.set(StateKey.of(k, Boolean.class), sb);
            default -> log.warn("Unsupported set value type: {}", v.getClass());
        }
    }
}
