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
import io.serverlessworkflow.api.types.WaitTask;

import java.time.Duration;
import java.util.List;

public class WaitTaskService {

    public static final ServiceDefinition DEFINITION = ServiceDefinition.of(
            "WaitTaskService",
            ServiceType.SERVICE,
            List.of(
                    HandlerDefinition.of(
                            "execute",
                            HandlerType.SHARED,
                            JacksonSerdes.of(WaitTask.class),
                            Serde.VOID,
                            HandlerRunner.of(WaitTaskService::execute, JacksonSerdeFactory.DEFAULT, HandlerRunner.Options.DEFAULT)
                    )
            )
    );

    public static void execute(WorkflowContext ctx, WaitTask task) {
        var wtc = task.getWait();
        var de = wtc.getDurationExpression();
        Duration resolvedDuration = Duration.ZERO;
        if (de != null) {
            resolvedDuration = Duration.parse(de);
        } else {
            var duri = wtc.getDurationInline();
            resolvedDuration = resolvedDuration.plusDays(duri.getDays())
                    .plusHours(duri.getHours())
                    .plusMinutes(duri.getMinutes())
                    .plusSeconds(duri.getSeconds())
                    .plusMillis(duri.getMilliseconds());
        }
        ctx.sleep(resolvedDuration);
    }
}
