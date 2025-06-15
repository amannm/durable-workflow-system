package com.amannmalik.workflow.runtime.task;

import com.amannmalik.workflow.runtime.DefinitionHelper;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import io.serverlessworkflow.api.types.WaitTask;

import java.time.Duration;

public class WaitTaskService {

    public static final ServiceDefinition DEFINITION =
            DefinitionHelper.taskService(WaitTaskService.class, WaitTask.class, WaitTaskService::execute);

    public static void execute(WorkflowContext ctx, WaitTask task) {
        var wtc = task.getWait();
        var de = wtc.getDurationExpression();
        Duration resolvedDuration = Duration.ZERO;
        if (de != null) {
            resolvedDuration = Duration.parse(de);
        } else {
            var duri = wtc.getDurationInline();
            resolvedDuration =
                    resolvedDuration
                            .plusDays(duri.getDays())
                            .plusHours(duri.getHours())
                            .plusMinutes(duri.getMinutes())
                            .plusSeconds(duri.getSeconds())
                            .plusMillis(duri.getMilliseconds());
        }
        ctx.sleep(resolvedDuration);
    }
}
