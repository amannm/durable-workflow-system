package com.amannmalik.workflow.runtime.task;

import dev.restate.sdk.HandlerRunner;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.endpoint.definition.HandlerDefinition;
import dev.restate.sdk.endpoint.definition.HandlerType;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import dev.restate.sdk.endpoint.definition.ServiceType;
import dev.restate.serde.Serde;
import dev.restate.serde.jackson.JacksonSerdeFactory;
import dev.restate.serde.jackson.JacksonSerdes;
import io.serverlessworkflow.api.types.SetTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SetTaskService {

    public static final ServiceDefinition DEFINITION = ServiceDefinition.of(
            "SetTaskService",
            ServiceType.SERVICE,
            List.of(
                    HandlerDefinition.of(
                            "execute",
                            HandlerType.SHARED,
                            JacksonSerdes.of(SetTask.class),
                            Serde.VOID,
                            HandlerRunner.of(SetTaskService::execute, JacksonSerdeFactory.DEFAULT, HandlerRunner.Options.DEFAULT)
                    )
            )
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
