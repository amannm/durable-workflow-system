package com.amannmalik.workflow.runtime.task;

import dev.restate.common.Request;
import dev.restate.common.Target;
import dev.restate.sdk.HandlerRunner;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.endpoint.definition.HandlerDefinition;
import dev.restate.sdk.endpoint.definition.HandlerType;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import dev.restate.sdk.endpoint.definition.ServiceType;
import dev.restate.serde.Serde;
import dev.restate.serde.TypeTag;
import dev.restate.serde.jackson.JacksonSerdeFactory;
import dev.restate.serde.jackson.JacksonSerdes;
import io.serverlessworkflow.api.types.EmitEventDefinition;
import io.serverlessworkflow.api.types.EmitTask;
import io.serverlessworkflow.api.types.EmitTaskConfiguration;
import io.serverlessworkflow.api.types.EventData;
import io.serverlessworkflow.api.types.EventProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EmitTaskService {

    public static final ServiceDefinition DEFINITION = ServiceDefinition.of(
            "EmitTaskService",
            ServiceType.SERVICE,
            List.of(
                    HandlerDefinition.of(
                            "execute",
                            HandlerType.SHARED,
                            JacksonSerdes.of(EmitTask.class),
                            Serde.VOID,
                            HandlerRunner.of(EmitTaskService::execute, JacksonSerdeFactory.DEFAULT, HandlerRunner.Options.DEFAULT)
                    )
            )
    );

    private static final Logger log = LoggerFactory.getLogger(EmitTaskService.class);

    public static void execute(WorkflowContext ctx, EmitTask task) {
        EmitTaskConfiguration emit = task.getEmit();
        EmitEventDefinition event = emit.getEvent();
        EventProperties with = event.getWith();
        EventData data = with.getData();
        Target target = Target.service("MyService", "myHandler");
        Object object = data.getObject();
        if (object instanceof String sv) {
            ctx.send(Request.of(target, TypeTag.of(String.class), TypeTag.of(String.class), sv));
        } else if (object instanceof Integer iv) {
            ctx.send(Request.of(target, TypeTag.of(Integer.class), TypeTag.of(Integer.class), iv));
        } else if (object instanceof Long lv) {
            ctx.send(Request.of(target, TypeTag.of(Long.class), TypeTag.of(Long.class), lv));
        } else {
            log.warn("Unsupported event payload type: {}", object.getClass());
        }
    }
}
