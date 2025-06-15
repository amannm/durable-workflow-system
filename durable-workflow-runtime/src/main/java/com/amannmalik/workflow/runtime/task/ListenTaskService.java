package com.amannmalik.workflow.runtime.task;

import com.amannmalik.workflow.runtime.DefinitionHelper;
import com.amannmalik.workflow.runtime.Services;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import io.serverlessworkflow.api.types.EventFilter;
import io.serverlessworkflow.api.types.ListenTask;
import io.serverlessworkflow.api.types.ListenTaskConfiguration;
import io.serverlessworkflow.api.types.ListenTo;
import io.serverlessworkflow.api.types.OneEventConsumptionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Listen task waits for an event of a given type using the EventBus service.
 */
public class ListenTaskService {

    public static final ServiceDefinition DEFINITION = DefinitionHelper.taskService(
            ListenTaskService.class,
            ListenTask.class,
            ListenTaskService::execute
    );

    private static final Logger log = LoggerFactory.getLogger(ListenTaskService.class);

    public static void execute(WorkflowContext ctx, ListenTask task) {
        ListenTaskConfiguration cfg = task.getListen();
        if (cfg == null) {
            return;
        }
        ListenTo to = cfg.getTo();
        if (to == null) {
            return;
        }
        String eventType = null;
        if (to.getOneEventConsumptionStrategy() != null) {
            OneEventConsumptionStrategy one = to.getOneEventConsumptionStrategy();
            EventFilter f = one.getOne();
            if (f != null && f.getWith() != null) {
                eventType = f.getWith().getType();
            }
        }
        if (eventType == null || eventType.isBlank()) {
            log.warn("Unsupported listen task configuration: {}", task);
            return;
        }
        // Await event from EventBus
        var future = Services.callVirtualObject(ctx, "EventBus", eventType, "await", null, String.class);
        if (future != null) {
            future.await();
        }
    }
}
