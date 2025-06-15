package com.amannmalik.workflow.runtime.task;

import com.amannmalik.workflow.runtime.DefinitionHelper;
import com.amannmalik.workflow.runtime.Services;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import io.serverlessworkflow.api.types.EmitEventDefinition;
import io.serverlessworkflow.api.types.EmitTask;
import io.serverlessworkflow.api.types.EmitTaskConfiguration;
import io.serverlessworkflow.api.types.EventData;
import io.serverlessworkflow.api.types.EventProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EmitTaskService {

    public static final ServiceDefinition DEFINITION = DefinitionHelper.taskService(
            EmitTaskService.class,
            EmitTask.class,
            EmitTaskService::execute
    );

    private static final Logger log = LoggerFactory.getLogger(EmitTaskService.class);

    public static void execute(WorkflowContext ctx, EmitTask task) {
        EmitTaskConfiguration emit = task.getEmit();
        EmitEventDefinition event = emit.getEvent();
        EventProperties with = event.getWith();
        if (with == null) {
            return;
        }
        EventData data = with.getData();
        String type = with.getType();
        if (type == null) {
            log.warn("Event type missing in emit task");
            return;
        }
        Object object = data == null ? null : data.getObject();
        String payload = (object == null) ? "" : object.toString();
        Services.callVirtualObject(ctx, "EventBus", type, "emit", payload, Void.class).await();
    }
}
