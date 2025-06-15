package com.amannmalik.workflow.runtime.task;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import com.amannmalik.workflow.runtime.DefinitionHelper;
import io.serverlessworkflow.api.types.SetTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetTaskService {

    public static final ServiceDefinition DEFINITION = DefinitionHelper.taskService(
            SetTaskService.class,
            SetTask.class,
            SetTaskService::execute
    );

    private static final Logger log = LoggerFactory.getLogger(SetTaskService.class);

    public static void execute(WorkflowContext ctx, SetTask task) {
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
